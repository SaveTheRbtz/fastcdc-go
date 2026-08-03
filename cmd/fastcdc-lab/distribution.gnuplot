if (ARGC != 2) {
    print "usage: gnuplot -c distribution.gnuplot distribution.csv distribution.svg"
    exit 1
}

set datafile separator comma
set terminal svg size 960,520 dynamic enhanced font "sans,11" background rgb "white"
set output ARG2
set title "FastCDC chunk-size distribution"
set xlabel "chunk size (bytes)"
set ylabel "probability mass per bin"
set key top right
set style fill solid 0.55 border

plot ARG1 using (column("upper_exclusive")-column("lower_inclusive") > 1 \
        ? (column("lower_inclusive")+column("upper_exclusive"))/2 : 1/0):\
        (column("observed_probability")):\
        (column("upper_exclusive")-column("lower_inclusive")) with boxes \
        linecolor rgb "#4c78a8" title "observed", \
     ARG1 using (column("upper_exclusive")-column("lower_inclusive") > 1 \
        ? (column("lower_inclusive")+column("upper_exclusive"))/2 : 1/0):\
        (column("analytical_probability")) with lines linewidth 2 \
        linecolor rgb "#e45756" title "independent-uniform model", \
     ARG1 using (column("upper_exclusive")-column("lower_inclusive") == 1 \
        ? column("lower_inclusive") : 1/0):\
        (column("observed_probability")) with points pointtype 7 pointsize 1.2 \
        linecolor rgb "#4c78a8" title "observed forced maximum", \
     ARG1 using (column("upper_exclusive")-column("lower_inclusive") == 1 \
        ? column("lower_inclusive") : 1/0):\
        (column("analytical_probability")) with points pointtype 2 pointsize 1.5 \
        linewidth 2 linecolor rgb "#e45756" title "model forced maximum"
