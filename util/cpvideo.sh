#!/bin/bash
month = 11
begin=1
end=11
for ((i=$begin;i<$end;i=i+1))
    do 
        dir_name = "raw_${month}_${day}"
        mkdir ./dataSet/vides/$dir_name
        cp /mnt/LiteOn_Videos/LiteOn_P1/LiteOn_P1_2019-"${month}"-"${i}"* /dataSet
    done

begin=1
end=30
tmp=9

for ((i=$begin;i<$end;i=i+1))
do
    echo $i



        
            
        # do
        #    cp -r "$file" /home/min/apnoms19/dataSet/videos/LiteOn_P
        # done

        # for file in /mnt/LiteOn_Videos/LiteOn_P1/LiteOn_P1_2019-11-"$i"*
        #     do 
        #         cp -r "$file" /home/min/apnoms19/dataSet/videos/LiteOn_P1_2019-11-"$i"
        #     done

done