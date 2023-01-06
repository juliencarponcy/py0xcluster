def _get_date_limits(self, start_date, end_date, batch_size):
    # Calculate the date limits for each batch
    date_delta = end_date - start_date
    num_batches = date_delta.days // batch_size
    for i in range(num_batches):
        yield (
            start_date + datetime.timedelta(days=i * batch_size),
            start_date + datetime.timedelta(days=(i + 1) * batch_size),
        )