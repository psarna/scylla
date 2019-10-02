/*
 * Copyright 2019 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "alternator/error.hh"
#include "log.hh"
#include <string_view>
#include <openssl/hmac.h>
#include <seastar/util/defer.hh>
#include "hashers.hh"
#include "bytes.hh"
#include "alternator/auth.hh"
#include <fmt/format.h>

namespace alternator {

static logging::logger alogger("alternator-auth");

static hmac_sha256_digest hmac_sha256(std::string_view key, std::string_view msg) {
    hmac_sha256_digest digest;
    unsigned int len = digest.size();

    HMAC_CTX* ctx = HMAC_CTX_new();
    auto guard = defer([ctx] { HMAC_CTX_free(ctx); });

    HMAC_Init_ex(ctx, reinterpret_cast<const unsigned char*>(key.data()), key.size(), EVP_sha256(), nullptr);
    HMAC_Update(ctx, reinterpret_cast<const unsigned char*>(msg.data()), msg.size());
    HMAC_Final(ctx, reinterpret_cast<unsigned char*>(digest.data()), &len);

    return digest;
}

static hmac_sha256_digest get_signature_key(std::string_view key, std::string_view date_stamp, std::string_view region_name, std::string_view service_name) {
    auto date = hmac_sha256("AWS4" + std::string(key), date_stamp);
    auto region = hmac_sha256(std::string_view(date.data(), date.size()), region_name);
    auto service = hmac_sha256(std::string_view(region.data(), region.size()), service_name);
    auto signing = hmac_sha256(std::string_view(service.data(), service.size()), "aws4_request");
    return signing;
}

static std::string apply_sha256(std::string_view msg) {
    sha256_hasher hasher;
    hasher.update(msg.data(), msg.size());
    sstring applied = to_hex(hasher.finalize());
    return std::string(applied.data(), applied.size());
}

std::string get_signature(std::string_view access_key_id, std::string_view host, std::string_view method, std::optional<std::string_view> content_type, std::string_view body_content, std::string_view region, std::string_view service, std::string_view query_string, std::string_view amz_target, std::string_view amz_date) {
    std::string secret_access_key = "whatever"; //fixme: that we need to get from DB :)
    std::string_view datestamp = amz_date.substr(0, 8);
    std::string canonical_uri = "/";

    // We seem to need a map of signed headers instead of generating them magically on the fly here.
    // Then, we serve a header only if it's in the map provided by the user
    std::string canonical_headers = content_type ? fmt::format("content-type:{}\n", *content_type) : "";
    canonical_headers += fmt::format("host:{}\nx-amz-date:{}\n", host, amz_date);
    if (content_type) {
        canonical_headers += fmt::format("x-amz-target:{}\n", amz_target);
    }
    std::string signed_headers = content_type ? "content-type;" : "";
    signed_headers += "host;x-amz-date";
    if (content_type) {
        signed_headers += ";x-amz-target";
    }

    std::string payload_hash = apply_sha256(body_content);

    std::string canonical_request = fmt::format("{}\n{}\n{}\n{}\n{}\n{}", method, canonical_uri, query_string, canonical_headers, signed_headers, payload_hash);
    alogger.warn("Canonical request: <{}>", canonical_request);

    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string credential_scope = fmt::format("{}/{}/{}/aws4_request", datestamp, region, service);
    std::string string_to_sign = fmt::format("{}\n{}\n{}\n{}", algorithm, amz_date, credential_scope,  apply_sha256(canonical_request));

    alogger.warn("string_to_sign: <{}>", string_to_sign);

    hmac_sha256_digest signing_key = get_signature_key(secret_access_key, datestamp, region, service);

    hmac_sha256_digest signature = hmac_sha256(std::string_view(signing_key.data(), signing_key.size()), string_to_sign);
    return to_hex(bytes_view(reinterpret_cast<const int8_t*>(signature.data()), signature.size()));
}


}
