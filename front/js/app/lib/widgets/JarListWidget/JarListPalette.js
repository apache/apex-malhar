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
/**
 * Palette for Jar List
 * 
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.lib.ListPalette;
var JarListPalette = BaseView.extend({
    
    events: {
        'click .inspectItem': 'goToJar',
        'click .removeJar': 'removeJar',
        'click .removeJars': 'removeJars',
        'click .refreshList': 'refreshList',
        'click .specifyDeps': 'specifyJarDeps'
    },
    
    goToJar: function() {
        var selected = this.getSelected();
        
        if (selected.length !== 1) return;
        
        this.nav.go('dev/jars/' + selected[0].get('name'), { trigger: true });
    },
    
    removeJar: function() {
        var model = this.getSelected()[0]
        
        if (confirm( DT.text('delete_jar_prompt') )) {
            model.destroy();
            this.collection.remove(model);
            this.collection.trigger('tabled:update');
        }
        
        this.render();
    },

    specifyJarDeps: function() {
        var selected = this.getSelected();

        if (selected.length !== 1) return;

        selected[0].specifyDependencies();
    },
    
    removeJars: function() {
        var models = this.getSelected();
        
        if (confirm( DT.text('delete_jars_prompt'))) {
            _.each(models, function(model) {
                model.destroy();
                this.collection.remove(model);
            }, this);
            this.collection.trigger('tabled:update');
        }
        
        this.render();
    },

    refreshList: function () {
        this.collection.fetch();
    },
    
    template: kt.make(__dirname+'/JarListPalette.html','_')
    
});
exports = module.exports = JarListPalette;