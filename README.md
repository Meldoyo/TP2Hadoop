# README
----

## What is it?
Hadoop TP2 by Anthony Da Mota and Paul-Adrien Cordonnier.


## How to use it?

Download the [latest release][last release] (TP2_Hadoop.jar).

The three M/R are available:

###1. Count first name by origin:

`hadoop jar TP2_Hadoop.jar fr.ece.hadoop.v2.CountNameByOrigin /res/prenoms.csv /user/your_user/out`

###2. Count number of first name by number of origin:

`hadoop jar TP2_Hadoop.jar fr.ece.hadoop.v2.CountNumberOfFirstNameByNumberOfOrigins /res/prenoms.csv /user/your_user/out`

###3. Proportion (in%) of male or female (now it does work !!):

`hadoop jar TP2_Hadoop.jar fr.ece.hadoop.v2.PercentageOfGender /res/prenoms.csv /user/your_user/out`


Two versions are available (v1 and v2) using both MapReduce API (mapred and mapreduce).
V2 contains much more comments.
## Authors
See [AUTHORS][AUTHORS].


[AUTHORS]: <AUTHORS>
[last release]: <https://github.com/Meldoyo/TP2Hadoop/releases/latest>