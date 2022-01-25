count=5
    for i in $(seq $count); do
        go test -run 2D -race
    done