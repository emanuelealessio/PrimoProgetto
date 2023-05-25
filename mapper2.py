#!/usr/bin/env python3

import sys

# Input format: ProductId, UserId, HelpfulnessNumerator, HelpfulnessDenominator, Score
# Output format: UserId\tHelpfulnessNumerator\tHelpfulnessDenominator

for line in sys.stdin:
    line = line.strip()
    fields = line.split(',')
    if len(fields) == 5:
        if fields[2].isdigit() and fields[3].isdigit():
            user_id = fields[1]
            helpfulness_numerator = int(fields[2])
            helpfulness_denominator = int(fields[3])
            print('{}\t{}\t{}'.format(user_id, helpfulness_numerator, helpfulness_denominator))

