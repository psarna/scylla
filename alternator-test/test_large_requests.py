# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests for authorization

import pytest
import requests

def gen_json(n):
    return '{"":'*n + '{}' + '}'*n

# Test that deeply nested objects (e.g. with depth of 50k) are parsed correctly
def test_deeply_nested_put(scylla_only, dynamodb, test_table):
    url = 'http://localhost:8000'
    big_json = gen_json(50000)
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}, "attribute":' + big_json + '}}'

    # NOTE: Signing routines use boto3 implementation details and may be prone
    # to unexpected changes
    class Request:
        url='http://localhost:8000'
        headers={'X-Amz-Target': 'DynamoDB_20120810.PutItem'}
        body=payload.encode(encoding='UTF-8')
        method='POST'
        context={}
        params={}
    req = Request()
    dynamodb.meta.client._request_signer.get_auth('alternator', 'us-east-1').add_auth(request=req)

    response = requests.post(req.url, headers=req.headers, data=req.body)
    print(response, response.text)

