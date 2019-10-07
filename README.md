[![Build Status](https://travis-ci.com/zooniverse/galaxy-zoo-panoptes.svg?token=GRsVDxkp9sFdzVbTHFVT&branch=master)](https://travis-ci.com/zooniverse/galaxy-zoo-panoptes)
<a href="https://codeclimate.com/repos/5ad86e7c56b0a20294008bc3/maintainability"><img src="https://api.codeclimate.com/v1/badges/8bc2a1735eb224ae42c3/maintainability" /></a>
<a href="https://codeclimate.com/repos/5ad86e7c56b0a20294008bc3/test_coverage"><img src="https://api.codeclimate.com/v1/badges/8bc2a1735eb224ae42c3/test_coverage" /></a>
[![astropy](http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat)](http://www.astropy.org/)

# Galaxy Zoo Reduction

Use this repo to convert Galaxy Zoo citizen scientist responses from Panoptes into aggregate classifications.
Responses are downloaded through API calls.

Using the API, responses can be collected in streaming mode - only pulling responses since the last download.
This is much quicker than using the Panoptes web interface for large projects (like Galaxy Zoo).

Collected responses are saved as json rows, which can easily by loaded into logstash/elasticsearch/kibana for a live-ish dashboard.

For historical compatability, the repo also includes code to process exports from Panoptes and Ouroborous.
This will not be generalised.

## Contributing

This could be generalised to any other decision-tree project by:
- Updating the example results in `data/examples` (used for the automatic streaming schema)
- Adding new (manual) workflow Schema objects to define questions/answers (only for aggregation)

Could be a good hack day project to refactor out the live streaming code...

## Installation

    pip install -r gzreduction/requirements.txt
    pip install -e gzreduction

Uses Spark, which relies on a slightly old java version. 
If you get weird java errors, check the Spark docs.

Creating a live dashboard (optional) will also require logstash/elasticsearch/kibana, and more setup.
Probably best not to try to do this for your own project unless you know what you're doing.

## Use

To download and aggregate responses:
    
    /data/miniconda3/envs/gz-panoptes/bin/python /Data/repos/gzreduction/gzreduction/main.py

or, if you're not me, run from the repo root:

    python gzreduction/main.py --working_dir=path/to/desired/or/current/results/dir

Many things happen at once:

- A new process begins downloading all new (since last run) responses by making classification and subject API calls to Panoptes (see `api_to_json.get_latest_classifications`). These raw responses are saved as json rows, with a few thousand per file. Place in `{working_dir}/raw`.

- Spark watches for new raw response files and, as each new file is created:
    - Extracts the key subject data (iauname, subject_id, etc) from each raw response. Place in `{working_dir}/subjects`
    - Converts the not-so-useful {Task: 'T1', Answer: '0 ![markdown](img)'} format annotations into {'Task': 'Cat?', Answer: 'Yes'} format, by asking the Panoptes API for the associated workflow information. I call these 'derived responses'. Place in `{working_dir}/derived`. 
    - 'Flattens' the derived responses (which are nested json) into a table of time/workflow/subject/task/answer. Place in `{working_dir}/flat`.

This all happens (by default) in streaming mode, so that only the latest responses are downloaded, subject-extracted, derived, and flattened. Once this is complete, streaming mode ends. 

Once streaming is complete, the flattened responses are aggregated (read: added up) and have a few extra columns calculated (e.g. vote fractions). This part will vary by project, and (currently) requires a manually-defined Schema object (see `gzreduction/schemas`) for the code to know what to do with each column. The aggregated responses are saved to `{working_dir}/aggregated.parquet.

*Bear in mind that if you change the code to produce different streaming results, you should delete the outputs and restart from scratch, or risk different parts of your data being processed in different ways!*

Finally, the aggregated responses are joined with the subject information extracted earlier to create a final table of aggregate responses and subject metadata/external keys.


## Why?

Exporting data for big projects is slow - potentially days or more - as everything is calculated from scratch.
Caesar avoids this, but:
- All the state lives server-side, which may or may not be what you want
- Executing custom code needs a server/Lambda (see Caesar docs on external reducers)
Very loosely, this is a simplified local version of Caesar aimed at more technical users.

New responses are identified outside of Spark, based on the last_ids in previously downloaded responses.
For everything else, Spark Structured Streaming keeps track of what has already been derived/extracted/flattened.
Spark processes each response file as it is created, faster than the responses are downloaded, giving results with a latency of a few minutes.
The final aggregation is done from scratch each time, but 

As a side benefit, you get a local copy of your Panoptes data.
The raw responses are effectively a log-based read-only copy of the Panoptes project DB.
The derived responses are similar, just with tweaked annotation formatting and markdown removed.
From here, it's easy to play more - for example, to load them into a dashboard...

## Dashboards (not generalised, personal notes only!)

To add GZ responses to elasticsearch and visualise with Kibana:

To get a new reduction, simly run (from the repo root):

    python gzreduction/main.py

To visualise progress on the elasticsearch dashboard, run:

    cd /data/programs
    ./elasticsearch*/bin/elasticsearch

    cd /data/programs
    ./kibana*/bin/kibana

    cd /data/programs
    ./logstash*/bin/logstash -f /data/programs/logstash*/test.conf

Make sure that disk has >50GB space, or elasticsearch will fail.

Excellent tutorial: https://www.elastic.co/blog/a-practical-introduction-to-logstash

# Old Docs Below (For Exports)

## Reduction to Single Votes

The first step is to convert the awkward raw classification exports from Panoptes or Ouroborous into a standard csv, where each row is a single vote and with columns `time`, `user`, `question`, and `answer`.

### Panoptes Reduction

The code for this step is under `reduction/panoptes`.
*TODO refactor to assume a file structure and simply take a date?*
*TODO convert to have date appended: '/data/repos/galaxy-zoo-panoptes/reduction/data/votes/panoptes_votes.csv'*

- Download a fresh DECALS DR5 workflow classification extract. Conventionally, place in `/Data/repos/galaxy-zoo-panoptes/reduction/data/raw/classifications/{YYYY}-{MM}-{DD}_panoptes_classifications.csv'`
- Point `settings.panoptes_old_style_classifications_loc` to this file.
- Download a fresh DECALS DR5 subjects export. Conventionally, place in `'/Data/repos/galaxy-zoo-panoptes/reduction/data/raw/subjects/{YYYY}-{MM}-{DD}_panoptes_subjects.csv'`
- Point `settings.panoptes_old_style_subjects_loc` to this file.
- Run `panoptes_extract_to_votes.py`. This will save a csv of votes to `settings.panoptes_votes_loc`. By default, this will overwrite!

Multiple choice questions will be removed.

This process currently takes ~1 hour and may be replaced with a PySpark routine.

### Ouroborous Reduction

The code for this step is under `reduction/ouroborous`.
You should not need to re-run this. These should be stable, as ouroborous is no longer running. However, should it be necessary:
- Ensure `settings.dr1_classifications_export_loc` and `settings.dr2_classifications_export_loc` point to the final exports from Ouroborous. 
- Run `ouroborous_extract_to_votes.py`. This will save a csv of votes to `settings.dr2_votes_loc`. By default, this will overwrite!

## Single Votes to Predictions

The code for this step is under `reduction/votes_to_predictions`.

Here, we reduce the standardised csv of single votes by subject to find the total votes per subject.
We then calculate useful extra columns e.g. majority prediction, counting uncertainty, etc.

The entry point is `votes_to_predictions.py`. This calls:
- `reduce_votes.py` to aggregate votes by subject, and then calculate total votes and vote fractions.
- `reduced_votes_to_predictions.py` to calculate predicted answers and uncertainty.

To execute, within `votes_to_predictions.py`:
- Set `new_panoptes` and/or `new_ouroborous` to True depending on which single vote csv you would like to find predictions from.
Assuming you are running Panoptes = True...
- Ensure `settings.panoptes_votes_loc` and/or `settings.dr2_votes_loc` points to the latest single vote csv(s). This is the output location for single vote csv(s), and so should already be correct.
- Point `settings.panoptes_aggregated_votes_loc` to where you would like to save the total votes per subject (before adding extra columns). By default, this will overwrite!
- Point `settings.panoptes_predictions_loc` to where you would like to save the total votes per subject including extra columns. 

This prediction table does not include either subject metadata from Galaxy Zoo or NSA catalog properties.
You will likely find it useful to join these to the prediction table.
- Under the folder `reduction`, run `add_catalog.py`. This will save a csv of the prediction table joined with GZ subject metadata and the NSA catalog to`settings.panoptes_predictions_with_catalog_loc`. By default, this will overwrite!
