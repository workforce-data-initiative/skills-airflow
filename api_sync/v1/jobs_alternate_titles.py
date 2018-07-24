import csv
import hashlib
import io
from sqlalchemy.orm import sessionmaker

from .models import JobAlternateTitle


def load_alternate_titles(storage, filename, db_engine):
    reader = csv.DictReader(io.BytesIO(storage.load(filename)), delimiter='\t')
    session = sessionmaker(db_engine)()
    for row in reader:
        if row['Original Title'] == row['Title']:
            continue
        title = row['Title']
        job = JobAlternateTitle(
            uuid=str(hashlib.md5(title.encode('utf-8')).hexdigest()),
            job_uuid=row['job_uuid'],
            title=title,
            nlp_a=row['nlp_a']
        )
        session.merge(job)
    session.commit()
