# format: {cesar_question: {'question': new_question, cesar_answer: new_answer}}
dr5_schema = {

    'T0-smooth-or-featured': {

        'question': 'smooth-or-featured',
        'answers': {
            'data.0': 'smooth',
            'data.1': 'featured-or-disk',
            'data.2': 'artifact'
        },

    },

    'T1-how-rounded': {

        'question': 'how-rounded',
        'answers': {
            'data.0': 'completely',
            'data.1': 'in-between',
            'data.2': 'cigar-shaped'
        },

    },

    'T2-edge-on': {

        'question': 'disk-edge-on',
        'answers': {
            'data.0': 'yes',
            'data.1': 'no',
        },
    },

    'T3-edge-on-bulge': {

        'question': 'edge-on-bulge',
        'answers': {
            'data.0': 'rounded',
            'data.1': 'boxy',
            'data.2': 'no-bulge'
        },

    },

    'T4-bar': {

        'question': 'bar',
        'answers': {
            'data.0': 'none',
            'data.1': 'weak',
            'data.2': 'strong'
        },

    },

    'T5-spiral-pattern': {

        'question': 'has-spiral-arms',
        'answers': {
            'data.0': 'yes',
            'data.1': 'no',
        },
    },

    'T6-spiral-tight': {

        'question': 'spiral-winding',
        'answers': {
            'data.0': 'tight',
            'data.1': 'medium',
            'data.2': 'loose'
        },

    },

    'T7-spiral-number': {

        'question': 'spiral-arm-count',
        'answers': {
            'data.0': '1',
            'data.1': '2',
            'data.2': '3',
            'data.3': '4',
            'data.4': 'more-than-4',
            'data.5': 'cant-tell'
        },

    },

    'T8-bulge-size': {

        'question': 'bulge-size',
        'answers': {
            'data.0': 'no-bulge',
            'data.1': 'small',
            'data.2': 'moderate',
            'data.3': 'large',
            'data.4': 'dominant',
        },
    },

    'T10-rare': {

        'question': 'rare-features',
        'answers': {
            'data.0': 'ring',
            'data.1': 'lens-or-arc',
            'data.2': 'irregular',
            'data.3': 'dust-lane',
            'data.4': 'overlapping',
            'data.5': 'something-else',
            'data.6': 'nothing-unusual',
        },
    },

    'T11-merging': {

        'question': 'merging',
        'answers': {
            'data.0': 'merger',
            'data.1': 'tidal-debris',
            'data.2': 'both',
            'data.3': 'neither',
        },
    }

}