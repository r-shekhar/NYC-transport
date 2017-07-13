def style_matplotlib():
    import matplotlib
    import matplotlib.pyplot as plt

    import seaborn.apionly as sns
    from matplotlib import rcParams

    import matplotlib.dates as mdates
    from matplotlib.colors import SymLogNorm as symlog

    plt.style.use('seaborn-darkgrid')

    rcParams['font.sans-serif'] = ('Helvetica', 'Arial', 'Open Sans', 'Bitstream Vera Sans')
    rcParams['font.size'] = 10
    rcParams['font.stretch'] = 'normal'
    rcParams['font.weight'] = 'normal'
    rcParams['axes.titlesize'] = 11

    rcParams['savefig.dpi'] = 150
    rcParams['figure.dpi'] = 150

    rcParams['grid.color'] = 'w'
    rcParams['grid.alpha'] = 0.7

    rcParams['xtick.direction'] = 'out'
    rcParams['xtick.major.size'] = 3
    rcParams['ytick.direction'] = 'out'
    rcParams['ytick.major.size'] = 3


    import os.path
    homedirpath = os.path.expanduser('~')
    fontdirpath = ''
    if '/Users/' in homedirpath:
        fontdirpath = os.path.join(homedirpath, 'Library/Fonts/')
    else:
        fontdirpath = os.path.join(homedirpath, '.fonts/')
    fontsize2 = 'size={0:0.1f}'.format(12)
    rcParams['mathtext.it'] = ((':family=sans-serif:style=normal:variant='
                                'normal:weight=normal:stretch=normal:file={0}/'
                                'HelveticaOblique.ttf:' +
                                fontsize2
                                ).format(fontdirpath))
    rcParams['mathtext.rm'] = ((':family=sans-serif:style=normal:variant='
                                'normal:weight=normal:stretch=normal:file={0}/'
                                'Helvetica.ttf:' +
                                fontsize2
                                ).format(fontdirpath))
    rcParams['mathtext.tt'] = ((':family=sans-serif:style=normal:variant='
                                'normal:weight=normal:stretch=normal:file={0}/'
                                'Helvetica.ttf:' +
                                fontsize2
                                ).format(fontdirpath))
    rcParams['mathtext.bf'] = ((':family=sans-serif:style=normal:variant='
                                'normal:weight=normal:stretch=normal:file={0}/'
                                'HelveticaBold.ttf:' +
                                fontsize2
                                ).format(fontdirpath))
    rcParams['mathtext.cal'] = ((':family=sans-serif:style=normal:variant='
                                 'normal:weight=normal:stretch=normal:file='
                                 '{0}/Helvetica.ttf:' +
                                 fontsize2
                                 ).format(fontdirpath))
    rcParams['mathtext.sf'] = ((':family=sans-serif:style=normal:variant='
                                'normal:weight=normal:stretch=normal:file={0}/'
                                'Helvetica.ttf:' +
                                fontsize2
                                ).format(fontdirpath))