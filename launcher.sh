#!/bin/sh

NEIGHBOURS="Output/Neighbours.txt"
ITER_PART="Output/Iter_"
TARGET="Output/FOUND"

mkdir -p "Output"

echo "First round: find neighbours"

if [ ! -f $NEIGHBOURS  ]; then
    cat Input/$1 | Streamer/NeighbourMapper.py | sort -n | Streamer/NeighbourReducer.py > $NEIGHBOURS
fi

i=0

if [ ! -f "$ITER_PART""$i"".txt" ]; then
    cp $NEIGHBOURS "$ITER_PART""$i"".txt"
fi

while [[ ! -f $TARGET ]]; do
    OLD_FILE="$ITER_PART""$i".txt
    old_i=$i
    i=$((i + 1))
    CURRENT_FILE="$ITER_PART"$i".txt"

    if [ ! -f $CURRENT_FILE ]; then
        echo "$old_i""-th round. Iterating..."
        cat $OLD_FILE | Streamer/IterMapper.py | sort -k 1 | Streamer/IterReducer.py > $CURRENT_FILE
    else
        echo "Skipping ""$old_i""-th round..."
    fi
done
