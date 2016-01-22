# Cloud-Computing-Hadoop
Hadoop Program for Cloud Computing

CS 6343: CLOUD COMPUTING
Project #1

♦ Input files
1)  A sanitized crime database from http://utdallas.edu/~ilyen/course/cloud/for15f/data.zip
2)  The source of this crime database was from http://www.police.uk/data
3)  An example input file: http://utdallas.edu/~ilyen/course/cloud/for15f/proj1-input.txt

♦ Write a MapReduce program to compute the total crime incidents of each crime type in each region
  1)  Region definition
  2)  Crime location is defined on a coordinate system (East, North)
  3)  East and North are defined by a 5-digit numerical value
  4)  Region definition 1: use the first digit of the coordinates only to define a region
      . (5xxxx, 7xxxx), (5xxxx, 3xxxx), (8xxxx, 6xxxx), each is one region
      . Supposedly there are 100 regions, but not all the numbers appear in the files
  5)  Region definition 2: use the first three digits of the coordinates to define a region
      . (535xx, 726xx) is one region
  6)  Consider other region definitions
   Crime types include: Anti-social behavior, Burglary, Criminal damage and arson, Drugs, Other
  theft, Public disorder and weapons, Robbery, Shoplifting, Vehicle crime, Violent crime, Other
  crime
