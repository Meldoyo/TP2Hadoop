# README
----

## What it is?
Hadoop TP2 by Anthony Da Mota and Paul-Adrien Cordonnier.


## How to use it?

Download the [latest release][last release] (TP2_Hadoop.jar).

The three M/R are available:

###1. Count first name by origin:

`hadoop jar TP2_Hadoop.jar fr.ece.hadoop.v1.CountNameByOrigin /res/prenoms.csv /user/your_user/out`

###2. Count number of first name by number of origin:

`hadoop jar TP2_Hadoop.jar fr.ece.hadoop.v1.CountNumberOfFirstNameByNumberOfOrigins /res/prenoms.csv /user/your_user/out`

###3. Proportion (in%) of male or female (which doesn't work):

`hadoop jar TP2_Hadoop.jar fr.ece.hadoop.v1.PercentageOfGender /res/prenoms.csv /user/your_user/out`

## Authors
See [AUTHORS][AUTHORS].


[AUTHORS]: <AUTHORS>
[last release]: <https://github.com/Meldoyo/TP2Hadoop/releases/latest>