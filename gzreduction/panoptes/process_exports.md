# Turn exports into the same outputs as API

    classifications_export=data/expert_classifications_export.csv
    subjects_export=data/latest_subjects_export.csv

    raw_dir=temp/raw
    flat_dir=temp/flat/latest_batch_export
    aggregated_loc=temp/latest_aggregated.parquet
    subjects_loc=temp/latest_subjects.parquet

OR
    classifications_export=data/expert_classifications_export.csv
    raw_dir=temp/calibration_experts/raw
    flat_dir=temp/calibration_experts/flat
    aggregated_loc=temp/calibration_experts/aggregated
    subjects_loc=temp/latest_subjects.parquet

OR

    classifications_export=data/volunteer_calibration_export.csv
    raw_dir=temp/calibration_volunteers/raw
    flat_dir=temp/calibration_volunteers/flat
    aggregated_loc=temp/calibration_volunteers/aggregated
    subjects_loc=temp/latest_subjects.parquet

OR

    base_dir=/media/walml/beta/galaxy_zoo/decals/dr5/volunteer_reduction/2020_09_25/

    classifications_export=$base_dir/decals-dr5-classifications.csv
    classifications_export=$base_dir/classic-classifications.csv
    classifications_export=$base_dir/enhanced-classifications.csv
    raw_dir=$base_dir/raw_{whatever}

    (then place in same raw folder)

    raw_dir=$base_dir/raw
    flat_dir=$base_dir/flat
    subjects_export=$base_dir/galaxy-zoo-subjects.csv
    subjects_loc=$base_dir/subjects/latest_subjects.parquet
    aggregated_loc=$base_dir/aggregated

Make the bulk export into something very much like the output of api/derived:

    python gzreduction/panoptes/extract/panoptes_extract_to_json.py  --export $classifications_export --save-dir $raw_dir

Also extract subjects:

    python gzreduction/panoptes/panoptes_export_to_subjects.py --subjects $subjects_export --output $subjects_loc

Flatten:

    python gzreduction/flatten.py --input $raw_dir --save-dir $flat_dir

Aggregate:

    python gzreduction/aggregate.py --input $flat_dir --subjects $subjects_loc --save-loc $aggregated_loc
