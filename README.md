[![Build Status](https://travis-ci.com/zooniverse/galaxy-zoo-panoptes.svg?token=GRsVDxkp9sFdzVbTHFVT&branch=master)](https://travis-ci.com/zooniverse/galaxy-zoo-panoptes)
<a href="https://codeclimate.com/repos/5ad86e7c56b0a20294008bc3/maintainability"><img src="https://api.codeclimate.com/v1/badges/8bc2a1735eb224ae42c3/maintainability" /></a>
<a href="https://codeclimate.com/repos/5ad86e7c56b0a20294008bc3/test_coverage"><img src="https://api.codeclimate.com/v1/badges/8bc2a1735eb224ae42c3/test_coverage" /></a>
[![astropy](http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat)](http://www.astropy.org/)

# Galaxy Zoo Panoptes

This repo contains everything needed for the Galaxy Zoo Panoptes project workflows and field guide. With this repo, one should be able to quickly (albeit somewhat tediously) replicate the Galaxy Zoo Panoptes project.

# Reduction 

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
- Set `new_panoptes` and/or `new_ourorborous` to True depending on which single vote csv you would like to find predictions from.
Assuming you are running Panoptes = True...
- Ensure `settings.panoptes_votes_loc` and/or `settings.dr2_votes_loc` points to the latest single vote csv(s). This is the output location for single vote csv(s), and so should already be correct.
- Point `settings.panoptes_aggregated_votes_loc` to where you would like to save the total votes per subject (before adding extra columns). By default, this will overwrite!
- Point `settings.panoptes_predictions_loc` to where you would like to save the total votes per subject including extra columns. 

This prediction table does not include either subject metadata from Galaxy Zoo or NSA catalog properties.
You will likely find it useful to join these to the prediction table.
- Under the folder `reduction`, run `add_catalog.py`. This will save a csv of the prediction table joined with GZ subject metadata and the NSA catalog to`settings.panoptes_predictions_with_catalog_loc`. By default, this will overwrite!
