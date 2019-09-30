


def limit_instances(count,cmin,cmax):
  cmin = 1 if not cmin else int(cmin)
  cmax = cmin if not cmax else max(int(cmax),cmin)
  count = max(min(int(count),cmax),cmin) if count else cmin
  return count,cmin,cmax

