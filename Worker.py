"""
Worker classes for template matching
"""

from itertools import izip

NUM_ANGLES = 181
NUM_AGES = 32
MAX_AGE = 3.5

PAD_DX = 200
PAD_DY = 200


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

      
class Reducer(Worker):

    def __init__(self, path):
        self.path = path
        self.best_results = None
        
    def compare(self, file1, file2, level):
        data1 = np.load(self.path + file1)
        data2 = np.load(self.path + file2)
        mask = data1[-1,:,:] > data2[-1,:,:]
        data2[mask,:] = data1[mask,:]
        
        os.remove(self.path + file1)
        os.remove(self.path + file2)
        
        filename = '{d}.npy'.format()
        np.save(self.path + filename, data2)
        
    def reduce(self):
        results = os.listdir(self.path)
        
        while len(results) > 1:
            results1 = results.pop()
            results2 = results.pop()
            self.compare(results1, results2)
            results = os.listdir(self.path)
        self.best_results = os.listdir(self.path)[0]
                
