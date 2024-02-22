select toString(count()) in ('1', 'x' || 'y') group by 'x' || 'y';
