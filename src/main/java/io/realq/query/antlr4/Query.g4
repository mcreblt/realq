grammar Query;

stmts
    : ';'* stmt ( ';'+ stmt )* ';'*
    ;

stmt
    : select 
    | createSource
    | createSink
    | createStream
    ;

createSource
    : K_CREATE K_SOURCE tableName '(' fieldDef ( ',' fieldDef )* ')' 
      source
    ;

createSink
    : K_CREATE K_SINK tableName  
      sink
      select
    ;

createStream
    : K_CREATE K_STREAM tableName select
    ;
    
source 
    : srcClass
      (serde)?
      (K_PARALLELISM parallelism)?
    ;

sink 
    : sinkClass
      (K_PARALLELISM parallelism)?
    ;

sinkClass
    : K_SINKCLASS className
      (K_SINKPROPERTIES '(' properties (',' properties)* ')')?
    ;

srcClass
    : K_SRCCLASS className
      (K_SRCPROPERTIES '(' properties (',' properties)* ')')?
    ;
    
serde
    : K_SERDE anyName
      (K_SERDEPROPERTIES '(' properties (',' properties)* ')')?
    ;

className : STRING_LITERAL;
    
select
	: K_SELECT selectExpr (',' selectExpr)* 
	  K_FROM tableName
	  (K_WHERE whereExpr)? 
	  (K_GROUP K_BY groupExpr)?
	  (K_PARALLELISM parallelism)?
	;

selectExpr
 : expr (K_AS alias)?
 ;

whereExpr
 : expr
 ;

groupExpr
 : expr  (',' expr)* (',' aggrWindow)?
 ;

parallelism
	: INT_LITERAL
	;

aggrWindow: K_TWINDOW functionParams ;

functionParams
    : '(' INT_PARAMS ')'
    ;
    
INT_PARAMS: INT (' ')*? ',' (' ')*? INT ;

functionParam
    : expr
    ;

//-> function(expr, expr)   // output
//  -> condition            // output, input(function_1) arg(function_1)
//    -> arg -> lit         // arg (condition)
//    -> operator -> op          // arg (condition)
//    -> function(expr)     // output, input(condition) arg(condition)
//        -> arg -> field   // input, arg (function_2)
// -> arg -> field          // input, arg (function_1)


expr
 : field # arg
 | literal # arg
 | (anyName|aggrName) '(' expr (',' expr )* ')' # function
 | expr operator expr # comparison
 | expr operatorLogical expr # logical
 | '(' expr ')' # parentheses
 ;
 
fieldDef
 : anyName type
 ; 

alias
 : anyName
 ;

type
 : (anyName|strucName);
 
field 
 : anyName
 ;

properties 
 : key '=' value  ;

literal 
 : STRING_LITERAL
 | BOOLEAN_LITERAL
 | NUMERIC_LITERAL
 | INT_LITERAL
 ;

key       : STRING_LITERAL;
value     : STRING_LITERAL;
aggrName  : K_COUNT | K_SUM | K_MAX;

anyName   : ID;
strucName : STRUC_TYPE;
tableName : ID;
spoutName : STRING_LITERAL;
operator  : LT | LT_EQ | GT | GT_EQ | EQ | NOT_EQ1 | NOT_EQ2;
operatorLogical : AND | OR | PLUS;

K_SELECT 		: S E L E C T;
K_CREATE	 	: C R E A T E;
K_STREAM 		: S T R E A M;
K_SOURCE 		: S O U R C E;
K_SINK          : S I N K;
K_FROM			: F R O M;
K_GROUP			: G R O U P;
K_BY			: B Y;
K_WHERE			: W H E R E;
K_COUNT			: C O U N T;
K_SUM			: S U M;
K_MAX			: M A X;
K_SINKCLASS      : S I N K C L A S S;
K_SINKPROPERTIES : S I N K P R O P E R T I E S;
K_SRCCLASS		: S R C C L A S S;
K_SRCPROPERTIES : S R C P R O P E R T I E S;
K_SERDE         : S E R D E C L A S S;
K_SERDEPROPERTIES : S E R D E P R O P E R T I E S;
K_AS			: A S;
K_VIEW          : V I E W;
K_TWINDOW       : W I N D O W;
K_PARALLELISM   : P A R A L L E L I S M;

INT_LITERAL
    : INT;

INT
   : [0-9]+
   ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

BOOLEAN_LITERAL
 : TRUE 
 | FALSE
 ;
 
NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

DOUBLE_LITERAL 
 : [0-9]+(.[0-9]+)?
 ;
 
STRUC_TYPE: ('INT'|'STRUCT<'.*?'>');

LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ : '=';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';
AND : A N D;
OR : O R;
PLUS: '+';
TRUE: T R U E;
FALSE: F A L S E;

ID        : [a-zA-Z_] [a-zA-Z_0-9.]*;
WS        : [ \t\n\r]+ -> skip;

fragment DIGIT : [0-9];
fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];