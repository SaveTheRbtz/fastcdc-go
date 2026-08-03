if (ARGC != 2) {
    print "usage: gnuplot -c distribution.gnuplot distribution.csv distribution.svg"
    exit 1
}

set datafile separator comma
set terminal svg size 960,520 dynamic enhanced font "sans,11" background rgb "white"
set output ARG2
set title sprintf("FastCDC chunk-size distribution (%s)", ARG1)
set xlabel "chunk size (bytes)"
set ylabel "probability"
set key top right
set style fill solid 0.55 border

plot ARG1 using ((column("lower_inclusive")+column("upper_exclusive"))/2):\
        (column("observed_probability")):\
        (column("upper_exclusive")-column("lower_inclusive")) with boxes \
        linecolor rgb "#4c78a8" title "observed", \
     ARG1 using ((column("lower_inclusive")+column("upper_exclusive"))/2):\
        (column("analytical_probability")) with lines linewidth 2 \
        linecolor rgb "#e45756" title "independent-uniform model"
