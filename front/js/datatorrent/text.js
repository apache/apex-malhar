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

var _ = require('underscore');
var formatters = require('./formatters');

/**
 * Text package.
 *
 * May be used for i18n in the future. For now, it is a
 * simple look-up of text items
 */

var textHash = {
    'ops_main_breadcrumb'        :  'applications',
    'delete_item'                :  'delete',
    'delete_jar_prompt'          :  'Are you sure you want to delete this jar file? This cannot be undone.',
    'delete_jars_prompt'         :  'Are you sure you want to delete multiple jar files at once? This cannot be undone.',
    'launch_app'                 :  'launch',
    'select an item'             :  'select an item',
    'inspect'                    :  'inspect',
    'no action available'        :  'no action available',
    'processed_per_sec'          :  'processed/s',
    'processed_total'            :  'total processed',
    'emitted_per_sec'            :  'emitted/s',
    'emitted_total'              :  'total emitted',
    'tuples_per_sec'             :  'tuples/s',
    'tuples_total'               :  'total tuples',
    'current_wid_title'          :  'current window id',
    'recovery_wid_title'         :  'recovery window id',
    'current_wid_label'          :  'current wID',
    'recovery_wid_label'         :  'recovery wID',
    'state_label'                :  'state',
    'status_label'               :  'status',
    'alloc_mem_mb_label'         :  'allocated mem. (mb)',
    'alloc_mem_label'            :  'allocated mem.',
    'remaining_licensed_mem'     :  'remaining licensed mem.',
    'free_mem_mb_label'          :  'free memory (mb)',
    'num_operators_label'        :  '# operators',
    'num_containers_label'       :  '# containers',
    'num_partitions_label'       :  '# partitions',
    'as_of_label'                :  'as of',
    'up_for_label'               :  'up for',
    'ended_at_label'             :  'ended at',
    'avg_app_age_label'          :  'avg app age',
    'latency_ms_label'           :  'latency (ms)',
    'cpu_percentage_label'       :  'cpu %',
    'failure_count_label'        :  'failures',
    'last_heartbeat_label'       :  'last heartbeat',
    'container_id_label'         :  'container id',
    'host_label'                 :  'host',
    'locality_not_assigned'      :  'AUTOMATIC',
    'overwrite_jar_warning'      :  'A jar with this name already exists and will be overwritten.',
    'dep_options_title'          :  'Options',
    'dep_choices_title'          :  'Choices',
    'filename_label'             :  'file name',
    'mod_date_label'             :  'last modified',
    'filesize_label'             :  'file size',
    'owner_label'                :  'owner',
    'specify_deps_success_title' :  'Dependencies jars specified',
    'specify_deps_success_text'  :  'The dependency jars you selected and ordered have been specified for the corresponding jar file.',
    'specify_deps_error_title'   :  'Error specifying dependencies',
    'specify_deps_error_text'    :  'An error occurred trying to specify the dependencies for the jar you have selected. ',
    'licensed_mem_bar_title'     :  function(agent) {
        if (!agent.percentUsedLicenseMB) {
            return 'Memory usage loading or unknown.';
        }
        return 'You are using ' + formatters.byteFormatter(agent.usedLicensedMB, 'mb') + ' of ' + formatters.byteFormatter(agent.totalLicensedMB, 'mb') + ' licensed memory.';
    },
    'kill_ctnr_sent_title'       :  'Kill Container Signal Sent',
    'kill_ctnr_sent_text'        :  'The signal to kill this container has been sent. It may take a few moments for the container to terminate.',
    'kill_ctnr_fail_title'       :  'Kill Container Signal Failed',
    'kill_ctnr_fail_text'       :   function(reason) {
        var message = 'An error occurred sending the kill signal to this container. ';
        if (reason) {
            message += 'Server response: ' + reason;
        }
        return message;
    }

};

// Inter-dependent text items
_.extend(textHash, {
    'specify_jars_instructions'  :  'Select one or more dependency jars from the following list labeled "' + textHash.dep_options_title + '", then order them as needed in the "' + textHash.dep_choices_title + '" list.',
});

function getTextItem(key) {
    return textHash[key] || key;
}

exports = module.exports = getTextItem;