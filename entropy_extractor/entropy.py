import numpy as np
def color_entropy(signal):
    '''
    function returns entropy of a signal
    signal must be a 1-D numpy array
    '''
    bins = 10
    try:
        hist = np.histogram(signal, bins=[i for i in range(0,256,bins)])
    except Exception as e:
        print(e)
        print(signal)
    
    propab=np.array([i/np.sum(hist[0]) for i in hist[0]])
    nonzero_p = np.nonzero(propab)
    
    ent=np.sum([p*np.log2(1.0/p) for p in propab[nonzero_p]])
    
    return ent

def edge_entropy(signal):
    '''
    function returns entropy of a signal
    signal must be a 1-D numpy array
    '''
    hist = signal.flatten()
    
    propab=np.array([i/np.sum(hist) for i in hist])
    
    nonzero_p = np.nonzero(propab)
    ent=np.sum([p*np.log2(1.0/p) for p in propab[nonzero_p]])

    return ent