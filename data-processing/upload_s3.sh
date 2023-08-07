suffix=$4
# sudo aws s3 cp s3://tpcds-data-$suffix/data/$1.dat ~/data/$1.dat

if [[ $2 == "1" ]]; then
    sudo aws s3 cp ../../data/$1.dat s3://tpcds-raw-$3-$suffix/$1.dat
else
    sudo ~/Ditto/build/split $1 $2
    num=$(($2-1))
    for i in $(seq 0 1 $num); do
        sudo aws s3 cp ../../data/$1_$i.dat s3://tpcds-raw-$3-$suffix/$1_$i.dat
    done
    if [[ $1 != "store" ]]; then
        sudo rm ../../data/$1_*.dat
    fi
fi