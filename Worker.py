"""
Worker classes for template matching
"""

from itertools import izip

NUM_ANGLES = 181
NUM_AGES = 32
MAX_AGE = 3.5

PAD_DX = 200
PAD_DY = 200

class CacheProcessor(object):

    def __init__(self, d, de, source, data_path, template_path):
        self.d = d
        self.de = de
        self.data_path = data_path
        self.template_path = template_path
        self.data = dem.DEMGrid(source)
        self.data._pad_boundary(PAD_DX, PAD_DY)
        self.nrows, self.ncols = self.data._griddata.shape
        self.Template = Scarp

    def cache_curvature(self):
        for angle in np.linspace(-np.pi/2, np.pi/2, NUM_ANGLES):
            c = self.obj._calculate_directional_laplacian_numexpr(angle)
            np.save(self.data_path + 'curv_' + str(angle) + '.npy', c)

    def cache_templates(self):

        for logage in np.linspace(0, MAX_AGE, NUM_AGES):
            for angle in np.linspace(-np.pi/2, np.pi/2, NUM_ANGLES):
                age = 10.**logage
                template = self.Template(self.d, age, angle, self.ncols, self.nrows, self.de).template()
                fname = self.template_path + 't_' + str(self.d) + 'm_kt' + str(logage) + '_ang' + str(angle) + '.npy'
                np.save(fname, template)


class Worker(object):

    def __init__(self):
        pass


class Matcher(Worker):
    
    def __init__(self, source):
        self.age = None
        self.angle = None
        self.path = '/efs/results/'
        self.filename = None 
        self.source = None
        self.dx = None
        self.dy = None
        
    def clip_results(pad_dx, pad_dy):
        i = pad_dx / self.dx
        j = pad_dy / self.dy
        self.results = self.results[:, j:-j, i:-i]

    def save_template_match(self):
        self.results = match_template(self.data, self.age, self.angle)
        self.clip_results(PAD_DX, PAD_DY)
        np.save(self.path + self.filename, self.results)

    def set_params(self, age, angle):
        self.age = age
        self.angle = angle
        self.filename = 'L0_' + str(self.age) + '_' + str(self.angle) + '.npy'

    def set_source(self, source):
        self.source = source
        data = dem.DEMGrid(source)
        self.dx = data._georef_info.dx
        self.dy = data._georef_info.dy

      
class TreeReducer(Worker):

    def __init__(self, path):
        self.path = path
        
    def compare(self, file1, file2, level):
        data1 = np.load(self.path + file1)
        data2 = np.load(self.path + file2)
        mask = data1[-1,:,:] > data2[-1,:,:]
        data2[mask,:] = data1[mask,:]
        
        os.remove(self.path + file1)
        os.remove(self.path + file2)
        
        filename = 'L{:d}_{:d}.npy'.format(level, os.getpid())
        np.save(self.path + , data2)
        
    def tree_reduce(self):
        results = os.listdir(self.path)
        level = 1
        
        while len(results) > 1:
            for results1, results2 in pairwise(results):
                level1 = results1[1]
                level2 = results2[1]
                if level1 == level2:
                    self.compare(results1, results2, level)
                    results = os.listdir(self.path)
            level += 1
        self.best_results = os.listdir(self.path)[0]
                

def pairwise(iterable):
    x = iter(iterable)
    return izip(x, x)
