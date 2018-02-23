#!/bin/bash

python skeleton_parser.py

sort RelationItem.dat | uniq > uniqItem.dat
sort RelationUser.dat | uniq > uniqUser.dat
sort RelationBid.dat | uniq > uniqBid.dat
sort RelationCategory.dat | uniq > uniqCat.dat
