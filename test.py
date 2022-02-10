from datetime import timedelta
start='0900'
end='2100'

delta_start=timedelta(
    minutes=int(start[-2:]),
    hours=int(start[:2])
)

delta_end=timedelta(
    minutes=int(end[-2:]),
    hours=int(end[:2])
)

print(str(delta_end-delta_start).zfill(8))
    