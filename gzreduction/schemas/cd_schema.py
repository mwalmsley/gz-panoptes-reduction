from gzreduction.schemas.schema import Schema, Question, Answer
# https://github.com/mwalmsley/gz-panoptes-reduction/blob/master/gzreduction/schemas/cd_schema.py


_questions = [
    Question(
        name='smooth-or-featured',
        raw_name='T0',
        answers=[
            Answer(
                name='smooth',
                raw_name='smooth'),
            Answer(
                name='featured-or-disk',
                raw_name='features or disk'),
            Answer(
                name='problem',
                raw_name='star, artifact, or bad zoom')
        ]
    ),
    Question(
        name='how-rounded',
        raw_name='T1',
        answers=[
            Answer(
                name='round',
                raw_name='completely round'),
            Answer(
                name='in-between',  # TODO remove dash from both DR2 and DR5
                raw_name='in between'),
            Answer(
                name='cigar-shaped',
                raw_name='cigar-shaped')
        ]
    ),
    Question(
        name='disk-edge-on',
        raw_name='T2',
        answers=[
            Answer(
                name='yes',
                raw_name='yes -  edge on disk'),  # TODO filter by date
            Answer(
                name='no',
                raw_name='no - something else')
        ]
    ),
    Question(
        name='edge-on-bulge',
        raw_name='T3',
        answers=[
            Answer(
                name='rounded',
                raw_name='rounded'),
            Answer(
                name='boxy',
                raw_name='boxy'),
            Answer(
                name='none',
                raw_name='no bulge')
        ]
    ),
    Question(
        name='bar',
        raw_name='T4',
        answers=[
            Answer(
                name='strong',
                raw_name='strong bar'),
            Answer(
                name='weak',
                raw_name='weak bar'),
            Answer(
                name='no',
                raw_name='no bar')
        ]
    ),
    Question(
        name='has-spiral-arms',
        raw_name='T5',
        answers=[
            Answer(
                name='yes',
                raw_name='yes'),
            Answer(
                name='no',
                raw_name='no')
        ]
    ),
    Question(
        name='spiral-winding',
        raw_name='T6',
        answers=[
            Answer(
                name='tight',
                raw_name='tight'),
            Answer(
                name='medium',
                raw_name='medium'),
            Answer(
                name='loose',
                raw_name='loose')
        ]
    ),
    Question(
        name='spiral-arm-count',
        raw_name='T7',
        answers=[
            Answer(
                name='1',
                raw_name='1'),
            Answer(
                name='2',
                raw_name='2'),
            Answer(
                name='3',
                raw_name='3'),
            Answer(
                name='4',
                raw_name='4'),
            Answer(
                name='more-than-4',
                raw_name='more than 4'),
            Answer(
                name='cant-tell',  # TODO strip apostrophes
                raw_name="can't tell")
        ]
    ),
    Question(
        name='bulge-size',
        raw_name='T8',
        answers=[
            Answer(
                name='none',
                raw_name='no bulge'),
            Answer(
                name='small',
                raw_name='small'),
            Answer(
                name='moderate',
                raw_name='moderate'),
            Answer(
                name='large',
                raw_name='large'),
            Answer(
                name='dominant',
                raw_name='dominant')
        ]
    ),
    Question(
        name='merging',
        raw_name='T11',
        answers=[
            Answer(  # warning - answers before and after switching this q will both be 'merging'
                name='merger',
                raw_name='merging'),
            Answer(
                name='major-disturbance',
                raw_name='major disturbance'),
            Answer(
                name='minor-disturbance',
                raw_name='minor disturbance'),
            Answer(
                name='none',
                raw_name='none')
        ]
    ),
    # question never used in anger
    # Question(
    #     name='lensing',
    #     raw_name='T12',
    #     answers=[
    #         Answer(  # warning - answers before and after switching this q will both be 'merging'
    #             name='yes',
    #             raw_name='yes'),
    #         Answer(
    #             name='no',
    #             raw_name='no'),
    #     ]
    # ),
    Question(
        name='clumps',
        raw_name='T13',
        answers=[
            Answer(  # warning - answers before and after switching this q will both be 'merging'
                name='yes',
                raw_name='bright clumps'),
            Answer(
                name='no',
                raw_name='none'),
        ]
    ),
    Question(
        name='problem',
        raw_name='T14',
        answers=[
            Answer(  # warning - answers before and after switching this q will both be 'merging'
                name='star',
                raw_name='star'),
            Answer(
                name='artifact',
                raw_name='non-star artifact'),
            Answer(
                name='zoom',
                raw_name='bad image zoom'),
        ]
    ),
    Question(
        name='artifact',
        raw_name='T15',
        answers=[
            Answer(  # warning - answers before and after switching this q will both be 'merging'
                name='non-star',   # some repetition here
                raw_name='non-star artifact'),
            Answer(
                name='satellite',
                raw_name='satellite trail'),
            Answer(
                name='scattered',
                raw_name='scattered light'),
            Answer(
                name='diffraction',
                raw_name='diffraction spike'),
            Answer(
                name='ray',
                raw_name='cosmic ray'),
            Answer(
                name='saturation',
                raw_name='saturation feature (bleed trail)'),
            Answer(
                name='other',
                raw_name='other / not sure')
        ]
    )
]

cd_schema = Schema(_questions)