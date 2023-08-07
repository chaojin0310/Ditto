#!/bin/bash
# Must be executed at the scripts directory, at the sudo mode
results_path=../results

if [[ $1 == "11" ]]; then
    echo -n > $results_path/effect_perf.log
    echo -n > $results_path/time_breakdown.log
elif [[ $1 == "14" ]]; then
    echo -n > $results_path/time_breakdown.log
elif [[ $1 == "8" ]]; then
    rm $results_path/q1*
    rm $results_path/q16*
    rm $results_path/q94*
    rm $results_path/q95*
fi