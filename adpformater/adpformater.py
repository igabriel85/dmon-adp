import pandas as pd

input_table = pd.read_csv("metrics.csv")


for index, row in input_table.iterrows():
    input_table = input_table.append([row]*9)

input_table = input_table.sort_values(['row ID'])
input_table = input_table.reset_index(drop=True)

for index, rows in input_table.iterrows():

    if int(index) > 59:
        print "Index to big!"
    time = rows[0].split(", ", 1) #In Knime row for timestamp is row(55) last one
    timeHour = time[1].split(":", 2)
    timeHourSeconds = timeHour[2].split(".", 1)
    timeHourSecondsDecimal = timeHour[2].split(".", 1)
    timeHourSecondsDecimal[0] = str(index)
    if len(timeHourSecondsDecimal[0]) == 1:
        timeHourSecondsDecimal[0] = '0%s' %timeHourSecondsDecimal[0]
    decimal = '.'.join(timeHourSecondsDecimal)
    timeHour[2] = decimal
    timenew = ':'.join(timeHour)
    time[1] = timenew
    finalString = ', '.join(time)
    input_table.set_value(index, 'row ID', finalString)

input_table.to_csv('out.csv')


