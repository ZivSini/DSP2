Ziv Sinianski - 313192841
Yonatan Hod - 311142806

Step 1
    Mapper:
        1. Filter all the un relevant 2-grams (such as stop words, punctuations etc.)
        2. Emit all the occurrences from the 2-gram to counters:
            * N - number of total 2-grams
            * C - the number of occurrences of the 2 words in that order
            * C1 - the number of occurrences of the first word as the first word in a 2-gram
            * C2 - the number of occurrences of the second word as the second word in a 2-gram

    Reducer:
        1. Aggregate the total number of occurrences for each type of counting (N, C etc.) and emit the result with the key of <first word> <decade>.
        2. In case of N (total number of 2-grams), write in configuration and update bounds of max and min decades if necessary

Step 2
    Mapper:
        1. Send all the data as it is.

    Reducer:
        1. For each key <first word> <decade>, get all the second words that share a 2-gram with the first (in that order)
           calculate the nPMI value for each pair (we know for sure, that the values are ordered by the second word,
           for it is always the first word for each value) and emit the results.
           Emit <decade, nPMI w1 w1>

Step 3
    Mapper:
        1. Invert the nPMI value (the values are sorted ascending, so we invert the nPMI value such that the actual
           values will be sorted descending).

    Reducer:
        1. Invert the nPMI value back to it's actual value, and emit the top 10 collocations with their nPMI value for
           each decade.

