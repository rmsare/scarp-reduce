"""
Worker classes for template matching
"""

class Worker(object):

    def __init__(self):
        pass


class Matcher(Worker):
    
    def __init__(self, source):
        self.age = None
        self.angle = None
        self.path = '/efs/results/'
        self.filename = 'fit_' + str(self.age) + '_' + str(self.angle) + '.npy'
        self.source = source
        
    def match_template(self):
        self.results = match_template(self.data, self.age, self.angle)
        
    def save_results(self):
        np.save(self.path + self.filename, self.results)
      
      
class TreeReducer(Worker):

    def __init__(self, path):
        self.path = path
        
    def compare(self, file1, file2):
        data1 = np.load(self.path + file1)
        data2 = np.load(self.path + file2)
        mask = data1[2,:,:] > data2[2,:,:]
        data2[mask,:] = data1[mask,:]
        
        os.remove(self.path + file1)
        os.remove(self.path + file2)
        
        filename = str(os.getpid()) + '.npy'
        np.save(self.path + , data2)
        
    def tree_reduce(self):
        results = os.listdir(self.path)
        
        while len(results) > 1:
            for results1, results2 in pairs(results):
                self.compare(results1, results2)
                results = os.listdir(self.path)
        self.best_results = os.listdir(self.path)[0]
                
