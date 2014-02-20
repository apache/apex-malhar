/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
var templates = DT.templates;
var formatters = DT.formatters;
var text = DT.text;

function nameFormatter(name, row) {
    
	if (row.get('depJar') ) {
		return name;
	}
	
    return templates.jar_view_link({
        name: name
    });
}

function sizeFormatter(size, row) {

    return formatters.byteFormatter(size, 'b');

}

exports = module.exports = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "name", key: "name", label: text('filename_label'), filter: "like", format: nameFormatter, sort_value: "a", sort: "string", width: 150 },
    { id: "modificationTime", label: text('mod_date_label'), key: "modificationTime", sort: "number", filter: "date", format: "timeStamp", width: 150 },
    { id: "size", key: "size", label: text('filesize_label'), sort: "number", filter: "number", format: sizeFormatter },
    { id: "owner", key: "owner", label: text('owner_label'), sort: "string", filter: "like", width: 60 }
]