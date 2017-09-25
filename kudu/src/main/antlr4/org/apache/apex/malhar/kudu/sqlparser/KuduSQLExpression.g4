/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/

grammar KuduSQLExpression;

kudusqlexpression: (WHITESPACE)* SELECT (WHITESPACE)+ columnnamesselect (tableclause)? (whereclause)? (endstatement)? (withoptionsclause)? (WHITESPACE)*;

columnnamesselect : '*' # ALL_COLUMNS_SELECT_EXP
             | colnameselectexpression # COLNAME_BASED_SELECT_EXP
             ;

colnameselectexpression : colnameselectexpression (WHITESPACE)* COMMA (WHITESPACE)* colnameselectexpression # SELECT_COMPLEX_COL_EXPRESSION
                    | idorcolumnname (WHITESPACE)+ AS (WHITESPACE)+ ID # SELECT_ALIAS_USED
                    | idorcolumnname # SELECT_ID_ONLY_USED_AS_COLUMN_NAME
                    ;

idorcolumnname:       ID
                    | '\'' (WHITESPACE)* FROM (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* WHERE (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* USING (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* OPTIONS (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* NOT (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* NULL (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* CONTROLTUPLE_MESSAGE (WHITESPACE)* '\''
                    | '\'' (WHITESPACE)* READ_SNAPSHOT_TIME (WHITESPACE)* '\''
              ;

tableclause : (WHITESPACE)+ FROM (WHITESPACE)+ ID;

whereclause : (WHITESPACE)+ WHERE (WHITESPACE)+ columnfilterexpression;

withoptionsclause: (WHITESPACE)+ USING (WHITESPACE)+ OPTIONS (WHITESPACE)+ keyvaluepair;


keyvaluepair:
              READ_SNAPSHOT_TIME (WHITESPACE)* EQUAL_TO (WHITESPACE)* INT #SET_READ_SNAPSHOT_TIME
             | CONTROLTUPLE_MESSAGE (WHITESPACE)* EQUAL_TO (WHITESPACE)* STRINGVAL #SET_CONTROL_TUPLE_MSG
             | keyvaluepair (WHITESPACE)+ COMMA (WHITESPACE)* keyvaluepair #SET_MULTI_OPTIONS
            ;

columnfilterexpression: columnfilterexpression (WHITESPACE)* AND (WHITESPACE)*  columnfilterexpression #FILTER_COMPLEX_FILTER_EXP
                      | idorcolumnname (WHITESPACE)* comparisionoperator (WHITESPACE)* anyvalue #FILTER_COMPARISION_EXP
                      | idorcolumnname (WHITESPACE)+ IS (WHITESPACE)+ NOT (WHITESPACE)+ NULL #IS_NOT_NULL_FILTER_EXP
                      | idorcolumnname (WHITESPACE)+ IS (WHITESPACE)+ NULL #IS_NULL_FILTER_EXP
                      | idorcolumnname (WHITESPACE)+ IN (WHITESPACE)+ listofanyvalue #IN_FILTER_EXP
                      ;


comparisionoperator: EQUAL_TO
                     | LESSER_THAN
                     | LESSER_THAN_OR_EQUAL
                     | GREATER_THAN
                     | GREATER_THAN_OR_EQUAL
                     ;
anyvalue: bool
        | numval
        | doubleval
        | floatval
        | stringval
        ;

bool: TRUE
    | FALSE;

numval: INT;

doubleval: INT '.' INT 'd'
         | '.' INT 'd'
         ;

floatval: INT '.' INT 'f'
        | '.' INT 'f'
        ;

stringval: STRINGVAL;

listofanyvalue: listofbools
              | listofnums
              | listoffloats
              | listofdoubles
              | listofstrings

              ;

listofbools: LPAREN  (WHITESPACE)* bool (WHITESPACE)* (COMMA (WHITESPACE)* bool)* (WHITESPACE)* RPAREN;
listofnums:  LPAREN  (WHITESPACE)* numval (WHITESPACE)* (COMMA (WHITESPACE)* numval)* (WHITESPACE)* RPAREN;
listoffloats: LPAREN  (WHITESPACE)* floatval (WHITESPACE)* (COMMA (WHITESPACE)* floatval)* (WHITESPACE)* RPAREN;
listofdoubles: LPAREN  (WHITESPACE)* doubleval (WHITESPACE)* (COMMA (WHITESPACE)* doubleval)* (WHITESPACE)* RPAREN;
listofstrings: LPAREN  (WHITESPACE)* stringval (WHITESPACE)* (COMMA (WHITESPACE)* stringval)* (WHITESPACE)* RPAREN;

endstatement: SEMICOLON;

SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];

AS: [Aa][Ss];

FROM: [Ff][Rr][Oo][Mm];

WHERE: [Ww][[Hh][Ee][Rr][Ee];

NOT: [Nn][Oo][Tt];

NULL: [Nn][Uu][Ll] [Ll];

IN: [Ii][Nn];

IS: [Ii][Ss];

AND: [Aa][Nn][Dd];

TRUE: [Tt][Rr][Uu][Ee];

FALSE: [Ff][Aa][Ll][Ss][Ee];

COMMA: ',';

SEMICOLON: ';';

EQUAL_TO: '=';

GREATER_THAN: '>';

LESSER_THAN: '<';

GREATER_THAN_OR_EQUAL: '>=';

LESSER_THAN_OR_EQUAL: '<=';

LPAREN: '[';

RPAREN: ']';

USING: [Uu][Ss][Ii][Nn][Gg];

OPTIONS: [Oo][Pp][Tt][Ii][Oo][Nn][Ss];

READ_SNAPSHOT_TIME: [Rr][Ee][Aa][Dd] '_' [Ss][Nn][Aa][Pp][Ss][Hh][Oo][Tt]'_'[Tt][Ii][Mm][Ee];

CONTROLTUPLE_MESSAGE: [Cc][Oo][Nn][Tt][Rr][Oo][Ll][Tt][Uu][Pp][Ll][Ee]'_'[Mm][Ee][Ss][Ss][Aa][Gg][Ee];

ID: ('a'..'z' | 'A'..'Z' | '_') ('a'..'z' | 'A'..'Z' | '_' | '0'..'9')*;

INT: '0' .. '9'+
   ;

WHITESPACE : ('\t' | ' ' | '\r' | '\n'| '\u000C');

STRINGVAL: '"' (ESC|.)*? '"';

fragment
ESC: '\\"' | '\\\\' ;


