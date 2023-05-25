#!/usr/bin/env python3

import sys

# Input format: UserId\tHelpfulnessNumerator\tHelpfulnessDenominator
# Output format: UserId\tApprezzamento

current_user = None
current_numerator = 0
current_denominator = 0

for line in sys.stdin:
    line = line.strip()
    user_id, numerator, denominator = line.split('\t')

    numerator = int(numerator)
    denominator = int(denominator)

    if current_user == user_id:
        current_numerator += numerator
        current_denominator += denominator
    else:
        if current_user:
            if current_denominator > 0:
                apprezzamento = float(current_numerator) / float(current_denominator)
            else:
                apprezzamento = 0.0
            print('{}\t{}'.format(current_user, apprezzamento))

        current_user = user_id
        current_numerator = numerator
        current_denominator = denominator

if current_user == current_user:
    if current_denominator > 0:
        apprezzamento = float(current_numerator) / float(current_denominator)
    else:
        apprezzamento = 0.0
    print('{}\t{}'.format(current_user, apprezzamento))


