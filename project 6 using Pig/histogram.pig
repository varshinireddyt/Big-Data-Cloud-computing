I = Load '$P' USING PigStorage(',') AS (red:long, green:long, blue:long); 
C = FOREACH I GENERATE (red+10000, green+20000, blue+30000) AS (i:tuple(red:long,green:long,blue:long));
R = FOREACH C GENERATE (i.red/10000,i.red%1000) AS (j:tuple(redtype:long,val:long));
G = FOREACH C GENERATE (i.green/10000,i.green%1000) AS (j:tuple(greentype:long,val:long));
B = FOREACH C GENERATE (i.blue/10000,i.blue%1000) AS (j:tuple(bluetype:long,val:long));
U = UNION R,G,B;
by_counter = GROUP U BY j; 
T = FOREACH by_counter { result = TOP(1, 0, $1); GENERATE FLATTEN(result),COUNT(U) AS count;}
O = Distinct T;
dump O;
STORE O INTO '$O' USING PigStorage(',');