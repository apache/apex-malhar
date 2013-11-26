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
var Base = require('./IntTextInputView');
var FloatTextInputView = Base.extend({
    _acceptedCodes: [8,9,16,17,18,37,38,39,40,48,49,50,51,52,53,54,55,56,57,91,190]
});
exports = module.exports = FloatTextInputView;