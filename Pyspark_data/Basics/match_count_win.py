"""Input :
Team1,Team2,Winner
-----------------
India,Aus,India
Srilanka,Aus,Aus
Srilanka,India,India

Output :
Team,Total_match,Total_win,Total_loss
--------------------------------------
India,2,2,0
Srilanka,2,0,2
Aus,2,1,1

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("matches").getOrCreate()
data=[("India","Aus","India"),("Srilanka","Aus","Aus"),("Srilanka","India","India")]
schema=["team1","team2","winner"]
df=spark.createDataFrame(data=data,schema=schema)
df.createOrReplaceTempView("matches")
# df.show()
df1=df.selectExpr("team1 as team","winner")
# df1.show()
df2=df.selectExpr("team2 as team","winner")
fullTeam=df1.unionAll(df2)
# fullTeam.show()
fcount=fullTeam.select("team").groupBy("team").count()
teamMatchPlayed=fcount.selectExpr("team","count as Teammatch")
# teamMatchPlayed.show()
teamMatchPlayed.createOrReplaceTempView("teamMatchPlayed")
dfwin=df.select("winner").groupBy("winner").count()
winCounts=dfwin.selectExpr("winner","count as winCount")
winCounts.createOrReplaceTempView("winCounts")

spark.sql("""select t.team,t.Teammatch,coalesce(w.winCount,0) as total_win,(t.Teammatch-coalesce(w.winCount,0)) as loss_match from teamMatchPlayed t
left join winCounts w on t.team=w.winner""").show()

spark.sql("""select t.team ,t.total_matches,coalesce(w.total_win,0) as total_win,(t.total_matches-coalesce(w.total_win,0)) as total_loss 
from 
(select team,count(*) as total_matches from 
(select team1 as team from matches union All select team2 as team from matches) group by team ) t 
left outer join (select winner,count(*) as total_win from matches group by winner) w 
on t.team=w.winner""")

spark.sql("""SELECT A.Team as TeamName,
 count(CASE WHEN A.Team=A.winner then 1 end) as Won,
 count(CASE WHEN A.Team<>A.winner AND A.winner<>'-' THEN 1 END) as Lost,
 count(CASE WHEN A.winner='-' then 1 END) as Draws
FROM (SELECT team1 as Team,winner
    FROM matches
    UNION ALL
    SELECT team2 as Team, winner 
    FROM matches
    ) A
GROUP BY A.Team""").show()

