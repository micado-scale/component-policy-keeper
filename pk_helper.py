def limit_instances(count,cmin,cmax):
  cmin = 1 if cmin is None else int(cmin)
  cmax = cmin if cmax is None else max(int(cmax),cmin)
  count = cmin if count is None else max(min(int(count),cmax),cmin)
  return count,cmin,cmax