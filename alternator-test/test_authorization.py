# Copyright 2019 ScyllaDB
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
from botocore.exceptions import ClientError
import boto3

# Test that trying to perform an operation signed with a wrong key
# will not succeed
def test_wrong_key_access(request, dynamodb):
    endpoints = dynamodb.meta.client.describe_endpoints()['Endpoints']
    # It is not strictly necessary that only a single endpoint be returned,
    # but this is what Amazon DynamoDB does today (and so does Alternator).
    assert len(endpoints) == 1
    for endpoint in endpoints:
        assert 'CachePeriodInMinutes' in endpoint.keys()
        address = endpoint['Address']
        # Check that the address is a valid endpoint by checking that we can
        # send it another describe_endpoints() request ;-) Note that the
        # address does not include the "http://" or "https://" prefix, and
        # we need to choose one manually.
        prefix = "https://" if request.config.getoption('https') else "http://"
        verify = not request.config.getoption('https')
        url = prefix + address
        with pytest.raises(ClientError, match='UnrecognizedClientException'):
            if address.endswith('.amazonaws.com'):
                boto3.client('dynamodb',endpoint_url=url, verify=verify, aws_access_key_id='wrong_id', aws_secret_access_key='wrong_key').describe_endpoints()
            else:
                boto3.client('dynamodb',endpoint_url=url, region_name='us-east-1', aws_access_key_id='whatever', aws_secret_access_key='wrong_key', verify=verify).describe_endpoints()
