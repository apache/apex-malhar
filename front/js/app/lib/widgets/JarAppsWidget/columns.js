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

function nameFormatter(name, row) {
    return templates.jar_app_view_link({
        appName: name,
        fileName: row.get('fileName')
    });
}

exports = module.exports = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "name", filter: "like", label: "App Name", key: "name", sort: "string", sort_value: "a", format: nameFormatter }
]