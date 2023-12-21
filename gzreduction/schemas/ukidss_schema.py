
from gzreduction.schemas.schema import Schema, Question, Answer

# format: {cesar_question: {'question': new_question, cesar_answer: new_answer}}
_questions = [
    Question(
        name='smooth-or-featured',
        raw_name='ukidss-0',
        answers=[
            Answer(
                name='smooth',
                raw_name='a-0'),
            Answer(
                name='featured-or-disk',
                raw_name='a-1'),
            Answer(
                name='artifact',
                raw_name='a-2')
            ]
    ),
    Question(
        name='how-rounded',
        raw_name='ukidss-7',
        answers=[
            Answer(
                name='round',
                raw_name='a-0'),
            Answer(
                name='in-between',
                raw_name='a-1'),
            Answer(
                name='cigar',
                raw_name='a-2')
        ]
    ),
    Question(
        name='disk-edge-on',
        raw_name='ukidss-1',
        answers=[
            Answer(
                name='yes',
                raw_name='a-0'),
            Answer(
                name='no',
                raw_name='a-1')
            ]
    ),
    Question(
        name='bulge-shape',
        raw_name='ukidss-8',
        answers=[
            Answer(
                name='round',
                raw_name='a-0'),
            Answer(
                name='boxy',
                raw_name='a-1'),
            Answer(
                name='no-bulge',
                raw_name='a-2')
        ]
    ),

    Question(
        name='bar',
        raw_name='ukidss-2',
        answers=[
            Answer(
                name='yes',
                raw_name='a-0'),
            Answer(
                name='no',
                raw_name='a-1')
        ]
    ),
    Question(
        name='has-spiral-arms',
        raw_name='ukidss-3',
        answers=[
            Answer(
                name='yes',
                raw_name='a-0'),
            Answer(
                name='no',
                raw_name='a-1')
        ]
    ),
    Question(
        name='spiral-winding',
        raw_name='ukidss-9',
        answers=[
            Answer(
                name='tight',
                raw_name='a-0'),
            Answer(
                name='medium',
                raw_name='a-1'),
            Answer(
                name='loose',
                raw_name='a-2')
            ]
    ),
    Question(
        name='spiral-arm-count',
        raw_name='ukidss-10',
        answers=[
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
                raw_name='a-4'),
            Answer(
                name='cant-tell',
                raw_name='a-5')
            ]
    ),
    Question(
        name='bulge-size',
        raw_name='ukidss-4',
        answers=[
            Answer(
                name='no',
                raw_name='a-0'),
            Answer(
                name='just-noticeable',
                raw_name='a-1'),
            Answer(
                name='obvious',
                raw_name='a-2'),
            Answer(
                name='dominant',
                raw_name='a-3')  
        ]
    ),
    Question(
        name='something-odd',
        raw_name='ukidss-5',
        answers=[
            Answer(
                name='yes',
                raw_name='a-0'),
            Answer(
                name='no',
                raw_name='a-1')
        ]
    )
]

ukidss_schema = Schema(questions=_questions)

#     'ukidss-6': {

#         'question': 'rare-features',
#         'answers': {
#             'a-0': 'ring',
#             'a-1': 'lens-or-arc',
#             'a-2': 'disturbed',
#             'a-3': 'irregular',
#             'a-4': 'other',
#             'a-5': 'merger',
#             'a-6': 'dust-lane',
#         },
#     },

# }