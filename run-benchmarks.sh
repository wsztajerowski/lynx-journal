#!/usr/bin/env bash

for tg in {1..5} ; do
java -jar jmh-benchmarks/target/jmh-benchmarks.jar MPSCFrameworkBenchmark \
  -f 1 \
  -prof "async:libPath=/home/jarek/tools/async-profiler-3.0-c1ed9b3-linux-x64/lib/libasyncProfiler.so;output=collapsed;event=cpu;dir=threads-${tg}" \
  -rf json \
  -rff "threads-${tg}/jmh-result.json" \
  -tg $(($tg ** 2))
done

(find . -type f -name "jmh-result.json" -path "./threads-*/*" \
  -exec jq '.[] | select(.benchmark=="pl.wsztajerowski.MPSCFrameworkBenchmark.producers") | [.threads,.primaryMetric.score,.primaryMetric.scoreError] | @csv' {} \;
) | tr -d '"' | sort -n -k 1 > plot_data.txt

cat << 'GNUPLOT' > plot_script.gp
set terminal pngcairo
set output 'output.png'
set title 'Line Chart'
set xlabel 'X'
set ylabel 'Y'
set grid
set datafile separator ","
plot 'plot_data.txt' using 1:2:3 with yerrorbars title 'Data' lw 2
GNUPLOT

# Run gnuplot
gnuplot plot_script.gp

# Clean up temporary files
#rm plot_data.txt plot_script.gp

#echo "Chart has been generated as output.png"
