from gzreduction.schemas.schema import Schema, Question, Answer


dr2_schema = Schema()


smooth_or_featured = Question(
    name='smooth-or-featured',
    raw_name='decals-0'
)
smooth_or_featured.set_answers([
    Answer(
        name='smooth',
        raw_name='a-0'),
    Answer(
        name='featured-or-disk',
        raw_name='a-1'),
    Answer(
        name='artifact',
        raw_name='star or artefact')
    ])
dr2_schema.add_question(smooth_or_featured)


how_rounded = Question(
    name='how-rounded',
    raw_name='decals-8'
)
how_rounded.set_answers([
    Answer(
        name='completely',
        raw_name='a-0'),
    Answer(
        name='in-between',
        raw_name='a-1'),
    Answer(
        name='cigar-shaped',
        raw_name='a-2')
    ])
dr2_schema.add_question(how_rounded)


disk_edge_on = Question(
    name='disk-edge-on',
    raw_name='decals-1'
)
disk_edge_on.set_answers([
    Answer(
        name='yes',
        raw_name='a-0'),
    Answer(
        name='no',
        raw_name='a-1')
    ])
dr2_schema.add_question(disk_edge_on)


edge_on_bulge = Question(
    name='edge-on-bulge',
    raw_name='decals-7'
)
edge_on_bulge.set_answers([
    Answer(
        name='rounded',
        raw_name='a-0'),
    Answer(
        name='boxy',
        raw_name='a-1'),
    Answer(
        name='none',
        raw_name='a-2')
    ])
dr2_schema.add_question(edge_on_bulge)


has_bar = Question(
    name='bar',
    raw_name='decals-2'
)
has_bar.set_answers([
    Answer(
        name='yes',
        raw_name='a-0'),
    Answer(
        name='no',
        raw_name='a-1')
    ])
dr2_schema.add_question(has_bar)


has_spiral_arms = Question(
    name='has-spiral-arms',
    raw_name='decals-3'
)
has_spiral_arms.set_answers([
    Answer(
        name='yes',
        raw_name='a-0'),
    Answer(
        name='no',
        raw_name='a-1')
    ])
dr2_schema.add_question(has_spiral_arms)


spiral_winding = Question(
    name='spiral-winding',
    raw_name='decals-5'
)
spiral_winding.set_answers([
    Answer(
        name='tight',
        raw_name='a-0'),
    Answer(
        name='medium',
        raw_name='a-1'),
    Answer(
        name='loose',
        raw_name='a-2')
    ])
dr2_schema.add_question(spiral_winding)


spiral_arm_count = Question(
    name='spiral-arm-count',
    raw_name='decals-6'
)
spiral_arm_count.set_answers([
    Answer(
        name='1',
        raw_name='a-0'),
    Answer(
        name='2',
        raw_name='a-1'),
    Answer(
        name='3',
        raw_name='a-2'),
    Answer(
        name='4',
        raw_name='a-3'),
    Answer(
        name='more-than-4',
        raw_name='a-4')
    ])
dr2_schema.add_question(spiral_arm_count)


bulge_size = Question(
    name='bulge-size',
    raw_name='decals-4'
)
bulge_size.set_answers([
    Answer(
        name='none',
        raw_name='a-0'),
    Answer(
        name='obvious',
        raw_name='a-1'),
    Answer(
        name='dominant',
        raw_name='a-2')
    ])
dr2_schema.add_question(bulge_size)


merging = Question(
    name='merging',
    raw_name='decals-9'
)
merging.set_answers([
    Answer(
        name='merger',
        raw_name='a-0'),
    Answer(
        name='tidal-debris',
        raw_name='a-1'),
    Answer(
        name='both',
        raw_name='a-2'),
    Answer(
        name='neither',
        raw_name='a-3')
    ])
dr2_schema.add_question(merging)


#     'decals-10': {

#         'question': 'rare-features',
#         'answers': {
#             'a-0': 'nothing-unusual',
#             'a-1': 'ring',
#             'a-2': 'lens-or-arc',
#             'a-3': 'irregular',
#             'a-4': 'something-else',
#             'a-5': 'dust-lane',
#             'a-6': 'overlapping',

#         },
#     },
