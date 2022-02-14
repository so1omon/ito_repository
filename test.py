from datetime import timedelta, datetime

now=datetime.now()
that_moment=now-timedelta(days=1)

print(that_moment.strftime('%Y%m%d'))