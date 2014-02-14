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
// TODO: improve these tests
var assert = require('assert');
var _ = require('underscore'), Backbone = require('backbone');
describe("the Tabled module", function() {
    
    var sandbox;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function() {
        sandbox.restore();
    });

    describe("a simple tabled view", function() {
        
        beforeEach(function() {
            this.Tabled = require('../');
            this.columns = [
                { id: "name", key: "name", label: "Name" },
                { id: "age", key: "age", label: "Age" }
            ]
            this.collection = new Backbone.Collection([
                { name: "andy", age: 24 },
                { name: "scott", age: 26 },
                { name: "tevya", age: 32 }
            ]);
            this.tabled = new this.Tabled({
                collection: this.collection,
                columns: this.columns,
                table_width: 500
            });
            this.$pg = $('<div id="playground"></div>');
            this.$pg.appendTo('#mocha');
            this.tabled.render().$el.appendTo(this.$pg);
        });
        
        it("should be a backbone view", function() {
            assert(this.tabled instanceof Backbone.View);
            // assert(this.tabled.$el instanceof $, "did not have an $el");
            // assert(this.tabled.el instanceof Element, "did not have an el");
            // assert.equal( typeof this.tabled.setElement, "function", "setElement was not a function");
        });
        
        it("should require that the data is a backbone collection", function(){
            var Tabled = this.Tabled;
            assert.throws(function() {
                new Tabled({
                    collection: [
                        {"name": "a"},
                        {"name": "b"}
                    ]
                })
            })
        });
        
        it("should allow chaining from the render method", function() {
            var same = this.tabled.render();
            assert(same === this.tabled, "render did not return this for chaining");
        });
        
        it("should make the columns list into a Backbone.Collection", function() {
            assert(this.tabled.columns instanceof Backbone.Collection, "tabled.columns was not a backbone collection");
        });
        
        it("should set a min column width on all columns", function() {
            this.tabled.columns.each(function(column){
                assert(column.get('min_column_width'), "A column did not have a min_column_width");
            })
        });
        
        it("should render .tabled, .thead, and .tbody element", function() {
            assert($(".tabled", this.$pg).length, ".tabled element not found");
            assert($(".thead", this.$pg).length, ".thead element not found");
            assert($(".tbody", this.$pg).length, ".tbody element not found");
        });
        
        it("should render as many columns as columns.length", function(){
            assert.equal($(".th").length, this.tabled.columns.length , "number of th's does not match column count");
        });
        
        it("should fill .th's with the labels", function() {
            var name = $(".th .cell-inner").filter(":eq(0)").text();
            var age = $(".th .cell-inner").filter(":eq(1)").text();
            assert.equal(name, this.columns[0].label, "'Name' was not the text of the first th");
            assert.equal(age, this.columns[1].label, "'Age' was not the text of the second th");
        });
        
        it("should render as many rows as the collection length", function() {
            assert.equal($(".tbody .tr", this.$pg).length, this.collection.length );
        });
        
        it("should have columns*rows number of td's", function() {
            assert.equal( $(".tbody .td", this.$pg).length, this.collection.length * this.tabled.columns.length, "table does not have the right amount of td's");
        });
        
        it("should store all widths in the columns", function() {
            this.tabled.columns.each(function(column){
                assert(column.get('width') != undefined);
                assert(typeof column.get('width') !== "undefined" );
            })
        })
        
        it("should add col-[id] classes to each row", function() {
            var ids = this.tabled.columns.pluck('id');
            var expected_length = this.collection.length;
            ids.forEach(function(id, i){
                assert.equal(expected_length, $(".tbody .td.col-"+id).length, "wrong number of .col-"+id+" tds");
            });
        });

        it("should fill tds with a .cell-inner element containing the row data", function() {
            var $rows = this.$pg.find(".tbody .tr");
            this.collection.each(function(data, i){
                var name = $rows.filter(":eq("+i+")").find(".td:eq(0) .cell-inner").text();
                var age = $rows.filter(":eq("+i+")").find(".td:eq(1) .cell-inner").text();
                assert.equal(data.get('name'), name, "row name did not match data name");
                assert.equal(data.get('age'), age, "row age did not match data age");
            }, this);
        });
        
        it("should set the widths of all .td elements to greater than or equal to the minimum column width", function() {
            var min_col_width = this.tabled.config.get("min_column_width");
            this.$pg.find(".th, .td").each(function(i, el) {
                assert( $(this).width() >= min_col_width , "cell width(s) were not greater than 0 ("+i+")" );
            });
        });
        
        it("should not have a filter row if there are no filters", function() {
            assert.equal( $('.filter-row', this.$pg ).length, 0 , "There is a filter row even though no columns have a filter");
        });
        
        it("should have size-adjustable columns", function() {
            var resizer = $('.resize:eq(0)', this.$pg);
            assert(resizer, "resizers should exist");
            var column = this.tabled.columns.at(0);
            var old_width = column.get('width');
            var mousedownEvt = $.Event("mousedown", {clientX: 0});
            var mousemoveEvt = $.Event("mousemove", {clientX: 10});
            resizer.trigger(mousedownEvt);
            $(window).trigger(mousemoveEvt);
            $(window).trigger("mouseup");
            assert(old_width != column.get('width'), "width of column object did not change");
            
            var new_width = column.get("width");
            $('.td-name', this.$pg).each(function(i,el){
                assert.equal(new_width, $(this).width(), "all cells of adjusted column are not the same size");
            })
        });
        
        it("colums should resize to content on dblclick", function() {
            var dblclick = $.Event("dblclick");
            var column = this.tabled.columns.get("name");
            var expected_width = 0;
            $(".td.col-name .cell-inner").each(function(i, el){
                expected_width = Math.max(expected_width, $(this).outerWidth(true), column.get('min_column_width'));
            });
            
            $(".th.col-name .resize").trigger(dblclick);
            assert.equal(column.get('width'), expected_width, "The column does not have the right width");
        });
        
        it("shouldn't allow columns to have a width of less than their min-width", function() {
            var column = this.tabled.columns.at(0);
            column.set( {'width': 0} , {validate: true} );
            assert(column.get('width') != 0, 'validation did not stop setting a bad width value');
        });
        
        it("rows should update when the models change", function() {
            var before = $(".td.col-age:eq(0)", this.$pg).text();
            this.collection.at(0).set("age", 25);
            var after = $(".td.col-age:eq(0)", this.$pg).text();
            assert(before != after, "age should have changed in the cell");
        });

        describe('the setLoading function', function() {

            it('should be a function', function() {
                expect(this.tabled.setLoading).to.be.a('function');     
            });

            it('should clear the tbody element', function() {
                this.tabled.setLoading();
                expect($('.tbody').html()).to.equal('<div class="tr loading-tr"><span class="tabled-spinner"></span> loading</div>');
            });
            
        });
        
        afterEach(function() {
            this.tabled.remove();
        });
        
    });
    
    describe("an advanced tabled view", function() {
        this.timeout(500);
        beforeEach(function(){
            this.Tabled = require('../');
            
            function inches2feet(inches, model){
                var feet = Math.floor(inches/12);
                var inches = inches % 12;
                return feet + "'" + inches + '"';
            }
            
            function feet_filter(term, value, formatted, model) {
                if (term == "tall") return value > 70;
                if (term == "short") return value < 69;
                return true;
            }
            
            this.columns = [
                { id: "selector", key: "selected", label: "", select: true, lock_width: true },
                { id: "first_name", key: "first_name", label: "First Name", sort: "string", filter: "like",  },
                { id: "last_name", key: "last_name", label: "Last Name", sort: "string", filter: "like",  },
                { id: "age", key: "age", label: "Age", sort: "number", filter: "number" },
                { id: "height", key: "height", label: "Height", format: inches2feet, filter: feet_filter, sort: "number" }
            ];
            this.collection = new Backbone.Collection([
                { id: 1, first_name: "andy",  last_name: "perlitch", age: 24 , height: 69 },
                { id: 2, first_name: "scott", last_name: "perlitch", age: 26 , height: 71 },
                { id: 3, first_name: "tevya", last_name: "robbins", age: 32  , height: 68 }
            ]);
            this.tabled = new this.Tabled({
                collection: this.collection,
                columns: this.columns,
                table_width: 500
            });
            this.$pg = $("#playground");
            this.tabled.render().$el.appendTo(this.$pg);
        });

        afterEach(function(){
            this.tabled.remove();
            this.tabled = null;
            this.collection = null;

        });
        
        it("should have a filter row with filter inputs", function() {
            assert.equal( $('.filter-row input').length, 4, "filter row not there or not enough filter inputs" );
        });
        
        it("should have checkboxes in the select column", function(){
            assert.equal(3, $('.col-selector input[type="checkbox"]').length, "wrong number of checkboxes found");
        });
        
        it("should make the 'selected' property on row models true when the cb is clicked", function() {
            $('.col-selector input[type="checkbox"]:eq(2)').trigger("mousedown");
            assert.equal(true, this.collection.get(3).selected, "Row.selected was not true");
        });
        
        it("should emit events when a checkbox is clicked", function(done) {
            this.collection.once("change_selected", function(model, value) {
                assert(this.collection.get(model) !== undefined, "model that changed was not in the collection");
                done();
            }, this);
            $('.col-selector input[type="checkbox"]:eq(0)').trigger('mousedown');
        });

        it("should lock the width of the columns that have lock_width enabled", function() {
            var col = this.tabled.columns.get('selector');
            col.set({'width': 200}, {validate: true});
            assert(col.get('width') != 200, "width of column changed when it should not have.");
        });
        
        it("should allow default filters", function(){
            $('.filter-row input:eq(1)').val('perli').trigger('click');
            assert.equal($(".tbody .tr").length, 2, "did not filter the rows down to 2");
        });
        
        it("should allow custom filters", function(){
            $('.filter-row input:eq(3)').val('tall').trigger('click');
            assert.equal($(".tbody .tr").length, 1, "did not filter the rows down to 1");
        });
        
        it("should emit a sort event when the header is clicked (mousedown, mouseup)", function(){
            var sort_spy = sandbox.spy();
            this.collection.on("sort", sort_spy);
            var vent_spy;
            var vent1 = $.Event("mousedown", {
                originalEvent: {
                    preventDefault: vent_spy = sandbox.spy()
                },
                clientX: 0
            });
            var vent2 = $.Event("mouseup", { clientX: 0 });
            var $header = $(".th-header:eq(4)", this.$pg);
            $header.trigger(vent1);
            $header.trigger(vent2);

            expect(vent_spy).to.have.been.calledOnce;
            expect(sort_spy).to.have.been.calledOnce;
        });
        
        it("should only fire one sort event when the header is clicked", function() {
            var click_spy = sandbox.spy();
            this.collection.on("sort", click_spy);

            var vent = $.Event("mousedown", {clientX: 0});
            var vent2 = $.Event("mouseup", {clientX: 0});
            var $header = $(".th-header:eq(4)", this.$pg);
            $header.trigger(vent);
            $header.trigger(vent2);

            expect(click_spy).to.have.been.calledOnce;
        });
        
        it("should not fire a sort event if the absolute value of the column change is greater than 8", function() {
            var click_spy = sandbox.spy();
            this.collection.on("sort", click_spy);

            var vent = $.Event("mousedown", {clientX: 0});
            var vent2 = $.Event("mouseup", {clientX: 9});
            var $header = $(".th-header:eq(4)", this.$pg);
            $header.trigger(vent);
            $header.trigger(vent2);

            expect(click_spy).not.to.have.been.called;
        });
        
        it("should have sortable columns", function() {
            var $th = $(".th-header:eq(4)", this.$pg);
            var down = $.Event("mousedown", {clientX: 300, originalEvent: { preventDefault: function() {} }});
            $th.trigger(down);
            var move = $.Event("mousemove", {clientX: 0});
            $th.trigger(move);
            $(window).trigger("mouseup");
            assert.equal('height', this.tabled.columns.col_sorts[0], "did not change column sort order");
        });
        
        it("should not sort rows when columns are being sorted", function() {
            var $th = $(".th-header:eq(4)", this.$pg);
            var down = $.Event("mousedown", {clientX: 300, originalEvent: { preventDefault: function() {} }});
            $th.trigger(down);
            var move = $.Event("mousemove", {clientX: 0});
            $th.trigger(move);
            $th.trigger("mouseup");
            assert.equal('', this.tabled.columns.get("height").get("sort_value"), "changed sort order when moving columns");
        });
        
    });
    
    describe("a tabled view with save_state enabled", function(){
        beforeEach(function(){
            this.Tabled = require('../');
            
            function inches2feet(inches, model){
                var feet = Math.floor(inches/12);
                var inches = inches % 12;
                return feet + "'" + inches + '"';
            }
            
            function feet_filter(term, value, formatted, model) {
                if (term == "tall") return value > 70;
                if (term == "short") return value < 69;
                return true;
            }
            
            this.columns = [
                { id: "selector", key: "selected", label: "", select: true },
                { id: "first_name", key: "first_name", label: "First Name", sort: "string", filter: "like", sort_value: "d" },
                { id: "last_name", key: "last_name", label: "Last Name", sort: "string", filter: "like",  },
                { id: "age", key: "age", label: "Age", sort: "number", filter: "number" },
                { id: "height", key: "height", label: "Height", format: inches2feet, filter: feet_filter, sort: "number" }
            ];
            this.collection = new Backbone.Collection([
                { id: 1, first_name: "andy",  last_name: "perlitch", age: 24 , height: 69, selected: false },
                { id: 2, first_name: "scott", last_name: "perlitch", age: 26 , height: 71, selected: false },
                { id: 3, first_name: "tevya", last_name: "robbins", age: 32  , height: 68, selected: true }
            ]);
            this.tabled = new this.Tabled({
                collection: this.collection,
                columns: this.columns,
                table_width: 500,
                save_state: true,
                id: "example1table",
                row_sorts: ["first_name"]
            });
            this.$pg = $("#playground");
            this.$pg.html(this.tabled.render().el);
        });
        
        it("should allow an initial sort to be specified", function() {
            var arr = [];
            $(".tbody .td.col-first_name .cell-inner").each(function(){
                arr.push($(this).text());
            });
            assert(arr.join(" ") == "tevya scott andy", "Did not sort by initial settings");
            
        });
        
        it("should save widths in stringified JSON", function(){
            this.tabled.columns.at(0).set('width', 200);
            assert(typeof localStorage.getItem('tabled.example1table') === "string");
            assert.doesNotThrow(
                function(){
                    var obj = JSON.parse(localStorage.getItem('tabled.example1table'))
                }
            );
            localStorage.removeItem('tabled.example1table');
        });
        
        it("should save widths as an object with column ids as keys and widths as values", function() {
            this.tabled.columns.at(0).set('width', 200);
            var obj = JSON.parse(localStorage.getItem('tabled.example1table'));
            var widths = obj.column_widths;
            assert.equal("object", typeof widths, "Widths not stored as an object");
            _.each(widths, function(val, key){
                assert(this.tabled.columns.get(key), "One or more keys on the widths state object was not tied to a column")
            }, this);
        });
        
        it("should re-apply saved widths from previous sessions", function(){
            assert.equal(this.tabled.columns.at(0).get('width'), 200, "Did not set width to saved state");
            localStorage.removeItem('tabled.example1table');
        });
        
        it("should save the order of columns if they change", function() {
            this.tabled.columns.at(0).sortIndex(1);
            var obj = JSON.parse(localStorage.getItem('tabled.example1table'));
            assert(obj.hasOwnProperty('column_sorts'), "state object should have column_sorts property");
            var sorts = obj.column_sorts;
            assert.equal(sorts[1],'selector', "selector should be in the 2nd slot");
        });
        
        it("should restore the column sort order from previous sessions", function() {
            var obj = JSON.parse(localStorage.getItem('tabled.example1table'));
            assert.equal(this.tabled.columns.at(1).get('id'), 'selector', "did not restore previous sort order");
        });
        
        it("should save the max_rows value if it has been changed", function() {
            this.tabled.config.set('max_rows', 12);
            var obj = JSON.parse(localStorage.getItem('tabled.example1table'));
            assert.equal(obj.max_rows, 12, "max_rows was not saved");
        });
        
        it("should restore the max_rows value upon re-initialization", function(){
            assert.equal(this.tabled.config.get('max_rows'), 12, "max_rows was not restored");
        });
                
        afterEach(function(){
            this.tabled.remove();
        });
        
    });
    
    describe("a tabled view with lots of updating rows", function() {
        beforeEach(function() {
            var Tabled = require('../');

            function inches2feet(inches, model){
                var feet = Math.floor(inches/12);
                var inches = inches % 12;
                return feet + "'" + inches + '"';
            }

            function feet_filter(term, value, formatted, model) {
                if (term == "tall") return value > 70;
                if (term == "short") return value < 69;
                return true;
            }

            var columns = [
                { id: "selector", key: "selected", label: "", select: true, width: 30, lock_width: true },
                { id: "first_name", key: "first_name", label: "First Name", sort: "string", filter: "like",  },
                { id: "last_name", key: "last_name", label: "Last Name", sort: "string", filter: "like",  },
                { id: "age", key: "age", label: "Age", sort: "number", filter: "number" },
                { id: "height", key: "height", label: "Height", format: inches2feet, filter: feet_filter, sort: "number" },
                { id: "weight", key: "weight", label: "Weight", filter: "number", sort: "number" }
            ];
            this.collection = new Backbone.Collection([]);
            this.tabled = new Tabled({
                collection: this.collection,
                columns: columns,
                table_width: 500,
                max_rows: 10
            });
            this.$pg = $("#playground");
            this.tabled.render().$el.appendTo(this.$pg);

            function genRow(id){

                var fnames = [
                    "joe",
                    "fred",
                    "frank",
                    "jim",
                    "mike",
                    "gary",
                    "aziz"
                ];

                var lnames = [
                    "sterling",
                    "smith",
                    "erickson",
                    "burke"
                ];

                var seed = Math.random();
                var seed2 = Math.random();
                var seed3 = Math.random();
                var seed4 = Math.random();

                var first_name = fnames[ Math.round( seed * (fnames.length -1) ) ];
                var last_name = lnames[ Math.round( seed2 * (lnames.length -1) ) ];

                return {
                    id: id,
                    selected: false,
                    first_name: first_name,
                    last_name: last_name,
                    age: Math.ceil(seed3 * 75) + 15,
                    height: Math.round( seed4 * 36 ) + 48,
                    weight: Math.round( seed4 * 130 ) + 90
                }
            }

            function genRows(num){
                var retVal = [];
                for (var i=0; i < num; i++) {
                    retVal.push(genRow(i));
                };
                return retVal;
            }
            this.genRows = genRows;
            this.collection.reset(genRows(200));
        });
        
        it("should render no more than the max number of rows", function(){
            assert( $(".tbody .tr", this.$pg).length <= 10 , "too many rows in the tbody");
        });
        
        it("should react to change in offset", function() {
            var init = $(".tbody .tr:eq(1)", this.$pg).html();
            this.tabled.config.set('offset', 1);
            var after = $(".tbody .tr:eq(0)", this.$pg).html();
            assert.equal(init, after, "row 1 should have shifted to 0");
        });
        
        afterEach(function() {
            this.tabled.remove();
        })
    });
    
    describe("the predefined formatting functions", function(){
        beforeEach(function(){
            this.Formats = require('../lib/Formats');
        });
        it("should be able to format a timestamp into 'X units ago'", function() {
            var timeSince = this.Formats.timeSince;
            var now = +new Date();
            var one_second_ago = now - 1000;
            var one_day_ago = now - 86400000;
            expect(timeSince(now)).to.match(/<span title="[\w\d\s:]{10,}">a moment ago<\/span>/);
            expect(timeSince(one_second_ago)).to.match(/<span title="[\w\d\s:]{10,}">1 second ago<\/span>/);
            expect(timeSince(one_day_ago)).to.match(/<span title="[\w\d\s:]{10,}">1 day ago<\/span>/);
        });
        afterEach(function() {
            delete this.Formats;
        })
    });
})