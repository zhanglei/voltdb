# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import os
from voltcli import utility

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Update the license of a running database.',
    arguments=(
        VOLT.StringArgument(
            'license_path',
            'license file (extension .xml)',
            min_count=1, max_count=1),
    )
)
def load(runner):
#    columns = [VOLT.FastSerializer.VOLTTYPE_NULL, VOLT.FastSerializer.VOLTTYPE_NULL]
#    catalog = None
#    deployment = None
#    configuration = runner.opts.configuration
#    configuration = runner.get_catalog()
#    deployment = VOLT.utility.File(configuration).read()
#    columns[1] = VOLT.FastSerializer.VOLTTYPE_STRING
#    params = [catalog, deployment]
    # call_proc() aborts with an error if the update failed.
#    runner.call_proc('@LoadLicense', columns, params)
#    runner.info('The load license update succeeded.')
    
    
    license_file = utility.File(runner.opts.license_path)
    try:
        license_file.open()
        xml_text = license_file.read()
        response = runner.call_proc(
            '@LoadLicense',
            [VOLT.FastSerializer.VOLTTYPE_STRING],
            [xml_text])
        print "Successfully loaded license"
    finally:
        license_file.close()
    
    
    
    
    
    
