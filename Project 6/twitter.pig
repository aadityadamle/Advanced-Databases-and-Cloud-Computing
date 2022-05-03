G = LOAD '$G' USING PigStorage(',') AS (user_id: int, follower_id: int);
M1 = GROUP G BY follower_id;
R1 = FOREACH M1 GENERATE group, COUNT(G); 
M2 = GROUP R1 BY $1;
R2 = FOREACH M2 GENERATE group, COUNT(R1);
STORE R2 INTO '$O' USING PigStorage (',');
