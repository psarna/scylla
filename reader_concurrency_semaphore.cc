/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/seastar.hh>
#include <seastar/core/print.hh>

#include "reader_concurrency_semaphore.hh"
#include "utils/exceptions.hh"
#include <random>

#include <seastar/core/sleep.hh>
#include "exceptions/exceptions.hh"

reader_permit::resource_units::resource_units(reader_concurrency_semaphore& semaphore, reader_resources res) noexcept
        : _semaphore(&semaphore), _resources(res) {
}

reader_permit::resource_units::resource_units(resource_units&& o) noexcept
    : _semaphore(o._semaphore)
    , _resources(std::exchange(o._resources, {})) {
}

reader_permit::resource_units::~resource_units() {
    reset();
}

reader_permit::resource_units& reader_permit::resource_units::operator=(resource_units&& o) noexcept {
    if (&o == this) {
        return *this;
    }
    reset();
    _semaphore = o._semaphore;
    _resources = std::exchange(o._resources, {});
    return *this;
}

void reader_permit::resource_units::add(resource_units&& o) {
    assert(_semaphore == o._semaphore);
    _resources += std::exchange(o._resources, {});
}

void reader_permit::resource_units::reset(reader_resources res) {
    _semaphore->consume(res);
    if (_resources) {
        _semaphore->signal(_resources);
    }
    _resources = res;
}

reader_permit::reader_permit(reader_concurrency_semaphore& semaphore)
    : _semaphore(&semaphore) {
}

future<reader_permit::resource_units> reader_permit::wait_admission(size_t memory, db::timeout_clock::time_point timeout) {
    return _semaphore->do_wait_admission(memory, timeout);
}

reader_permit::resource_units reader_permit::consume_memory(size_t memory) {
    return consume_resources(reader_resources{0, ssize_t(memory)});
}

reader_permit::resource_units reader_permit::consume_resources(reader_resources res) {
    _semaphore->consume(res);
    return resource_units(*_semaphore, res);
}

void reader_concurrency_semaphore::signal(const resources& r) noexcept {
    _resources += r;
    while (!_wait_list.empty() && has_available_units(_wait_list.front().res)) {
        auto& x = _wait_list.front();
        _resources -= x.res;
        try {
            x.pr.set_value(reader_permit::resource_units(*this, x.res));
        } catch (...) {
            x.pr.set_exception(std::current_exception());
        }
        _wait_list.pop_front();
    }
}

reader_concurrency_semaphore::~reader_concurrency_semaphore() {
    broken(std::make_exception_ptr(broken_semaphore{}));
}

reader_concurrency_semaphore::inactive_read_handle reader_concurrency_semaphore::register_inactive_read(std::unique_ptr<inactive_read> ir) {
    // Implies _inactive_reads.empty(), we don't queue new readers before
    // evicting all inactive reads.
    if (_wait_list.empty()) {
        const auto [it, _] = _inactive_reads.emplace(_next_id++, std::move(ir));
        (void)_;
        ++_inactive_read_stats.population;
        return inactive_read_handle(*this, it->first);
    }

    // The evicted reader will release its permit, hopefully allowing us to
    // admit some readers from the _wait_list.
    ir->evict();
    ++_inactive_read_stats.permit_based_evictions;
    return inactive_read_handle();
}

std::unique_ptr<reader_concurrency_semaphore::inactive_read> reader_concurrency_semaphore::unregister_inactive_read(inactive_read_handle irh) {
    if (irh && irh._sem != this) {
        throw std::runtime_error(fmt::format(
                    "reader_concurrency_semaphore::unregister_inactive_read(): "
                    "attempted to unregister an inactive read with a handle belonging to another semaphore: "
                    "this is {} (0x{:x}) but the handle belongs to {} (0x{:x})",
                    name(),
                    reinterpret_cast<uintptr_t>(this),
                    irh._sem->name(),
                    reinterpret_cast<uintptr_t>(irh._sem)));
    }

    if (auto it = _inactive_reads.find(irh._id); it != _inactive_reads.end()) {
        auto ir = std::move(it->second);
        _inactive_reads.erase(it);
        --_inactive_read_stats.population;
        return ir;
    }
    return {};
}

bool reader_concurrency_semaphore::try_evict_one_inactive_read() {
    if (_inactive_reads.empty()) {
        return false;
    }
    auto it = _inactive_reads.begin();
    it->second->evict();
    _inactive_reads.erase(it);

    ++_inactive_read_stats.permit_based_evictions;
    --_inactive_read_stats.population;

    return true;
}

class queue_throttler {
    static constexpr ssize_t max_error_response_delay_ms = 1000;
    ssize_t _severity;
public:
    queue_throttler(ssize_t severity) : _severity(severity) {}
    bool should_throttle() {
        static thread_local std::default_random_engine random_engine{std::random_device{}()};
        static thread_local std::uniform_int_distribution<size_t> random_dist{0, 99};
        return _severity > 0 && random_dist(random_engine) < size_t(_severity);
    }
    future<reader_permit::resource_units> throttle() {
        return seastar::sleep(std::chrono::milliseconds(std::min(10 * _severity, max_error_response_delay_ms))).then([] {
            return make_exception_future<reader_permit::resource_units>(
                    std::make_exception_ptr(exceptions::overloaded_exception()));
        });
    }
};

future<reader_permit::resource_units> reader_concurrency_semaphore::do_wait_admission(size_t memory, db::timeout_clock::time_point timeout) {
    queue_throttler throttler(_wait_list.size() - _max_queue_length);
    if (throttler.should_throttle()) {
        if (_prethrow_action) {
            _prethrow_action();
        }
        return throttler.throttle();
    }
    auto r = resources(1, static_cast<ssize_t>(memory));
    auto it = _inactive_reads.begin();
    while (!may_proceed(r) && it != _inactive_reads.end()) {
        auto ir = std::move(it->second);
        it = _inactive_reads.erase(it);
        ir->evict();

        ++_inactive_read_stats.permit_based_evictions;
        --_inactive_read_stats.population;
    }
    if (may_proceed(r)) {
        _resources -= r;
        return make_ready_future<reader_permit::resource_units>(reader_permit::resource_units(*this, r));
    }
    promise<reader_permit::resource_units> pr;
    auto fut = pr.get_future();
    _wait_list.push_back(entry(std::move(pr), r), timeout);
    return fut;
}

reader_permit reader_concurrency_semaphore::make_permit() {
    return reader_permit(*this);
}

void reader_concurrency_semaphore::broken(std::exception_ptr ex) {
    while (!_wait_list.empty()) {
        _wait_list.front().pr.set_exception(std::make_exception_ptr(broken_semaphore{}));
        _wait_list.pop_front();
    }
}

// A file that tracks the memory usage of buffers resulting from read
// operations.
class tracking_file_impl : public file_impl {
    file _tracked_file;
    reader_permit _permit;

public:
    tracking_file_impl(file file, reader_permit permit)
        : _tracked_file(std::move(file))
        , _permit(std::move(permit)) {
        _memory_dma_alignment = _tracked_file.memory_dma_alignment();
        _disk_read_dma_alignment = _tracked_file.disk_read_dma_alignment();
        _disk_write_dma_alignment = _tracked_file.disk_write_dma_alignment();
    }

    tracking_file_impl(const tracking_file_impl&) = delete;
    tracking_file_impl& operator=(const tracking_file_impl&) = delete;
    tracking_file_impl(tracking_file_impl&&) = default;
    tracking_file_impl& operator=(tracking_file_impl&&) = default;

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->write_dma(pos, std::move(iov), pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_tracked_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_tracked_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_tracked_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_tracked_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_tracked_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_tracked_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_tracked_file)->close();
    }

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return get_file_impl(_tracked_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_tracked_file)->list_directory(std::move(next));
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_tracked_file)->dma_read_bulk(offset, range_size, pc).then([this, units = _permit.consume_memory(range_size)] (temporary_buffer<uint8_t> buf) {
            return make_ready_future<temporary_buffer<uint8_t>>(make_tracked_temporary_buffer(std::move(buf), _permit));
        });
    }
};

file make_tracked_file(file f, reader_permit p) {
    return file(make_shared<tracking_file_impl>(f, std::move(p)));
}
