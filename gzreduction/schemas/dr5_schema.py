from gzreduction.schemas.schema import Schema, Question, Answer


dr5_schema = Schema()

smooth_or_featured = Question(
    name='smooth-or-featured',
    raw_name='T0'
)
smooth_or_featured.set_answers([
    Answer(
        name='smooth',
        raw_name='smooth'),
    Answer(
        name='featured-or-disk',
        raw_name='features or disk'),
    Answer(
        name='artifact',
        raw_name='star or artifact')
    ])
dr5_schema.add_question(smooth_or_featured)


how_rounded = Question(
    name='how-rounded',
    raw_name='T1'
)
how_rounded.set_answers([
    Answer(
        name='round',
        raw_name='completely round'),
    Answer(
        name='in-between',  # TODO remove dash from both DR2 and DR5
        raw_name='in between'),
    Answer(
        name='cigar-shaped',
        raw_name='cigar-shaped')
    ])
dr5_schema.add_question(how_rounded)


disk_edge_on = Question(
    name='disk-edge-on',
    raw_name='T2'
)
disk_edge_on.set_answers([
    Answer(
        name='yes',
        raw_name='yes -  edge on disk'),  # TODO filter by date
    Answer(
        name='no',
        raw_name='no - something else')
    ])
dr5_schema.add_question(disk_edge_on)


edge_on_bulge = Question(
    name='edge-on-bulge',
    raw_name='T3'
)
edge_on_bulge.set_answers([
    Answer(
        name='rounded',
        raw_name='rounded'),
    Answer(
        name='boxy',
        raw_name='boxy'),
    Answer(
        name='none',
        raw_name='no bulge')
    ])
dr5_schema.add_question(edge_on_bulge)


has_bar = Question(
    name='bar',
    raw_name='T4'
)
has_bar.set_answers([
    Answer(
        name='strong',
        raw_name='strong bar'),
    Answer(
        name='weak',
        raw_name='weak bar'),
    Answer(
        name='no',
        raw_name='no bar')
    ])
dr5_schema.add_question(has_bar)


has_spiral_arms = Question(
    name='has-spiral-arms',
    raw_name='T5'
)
has_spiral_arms.set_answers([
    Answer(
        name='yes',
        raw_name='yes'),
    Answer(
        name='no',
        raw_name='no')
    ])
dr5_schema.add_question(has_spiral_arms)


spiral_winding = Question(
    name='spiral-winding',
    raw_name='T6'
)
spiral_winding.set_answers([
    Answer(
        name='tight',
        raw_name='tight'),
    Answer(
        name='medium',
        raw_name='medium'),
    Answer(
        name='loose',
        raw_name='loose')
    ])
dr5_schema.add_question(spiral_winding)


spiral_arm_count = Question(
    name='spiral-arm-count',
    raw_name='T7'
)
spiral_arm_count.set_answers([
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
    ])
dr5_schema.add_question(spiral_arm_count)


bulge_size = Question(
    name='bulge-size',
    raw_name='T8'
)
bulge_size.set_answers([
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
    ])
dr5_schema.add_question(bulge_size)

# There's no T9, but Panoptes skips to T10. Interesting behaviour...

# T10 is 'rare features' and not currently aggregated

merging = Question(
    name='merging',
    raw_name='T11'
)
merging.set_answers([
    Answer(  # warning - answers before and after switching this q will both be 'merging'
        name='merger',
        raw_name='merging'),
    # old answers
    Answer(
        name='tidal-debris-v1',
        raw_name='tidal debris'),
    Answer(
        name='both-v1',
        raw_name='both (merger & tidal debris)'),
    Answer(
        name='neither-v1',
        raw_name='neither (no merger & no tidal debris)'),
    # new answers
    Answer(
        name='major-disturbance',
        raw_name='major disturbance'),
    Answer(
        name='minor-disturbance',
        raw_name='minor disturbance'),
    Answer(
        name='none',
        raw_name='none')
    ])
dr5_schema.add_question(merging)
