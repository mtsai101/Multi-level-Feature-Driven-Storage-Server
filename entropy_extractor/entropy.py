import numpy as np
"""
    function returns entropy of a signal
    signal must be a 1-D numpy array
"""
def color_entropy(signal):

    bins = 10
    try:
        hist = np.histogram(signal, bins=[i for i in range(0,256,bins)])
        propab=np.array([i/np.sum(hist[0]) for i in hist[0]])
        nonzero_p = np.nonzero(propab)
        ent=np.sum([p*np.log2(1.0/p) for p in propab[nonzero_p]])
        return ent
    except Exception as e:
        print(e)
        print(signal)
    
    
    
    

def edge_entropy(signal):
    try:
        hist = signal.flatten()
        
        propab=np.array([i/np.sum(hist) for i in hist])
        
        nonzero_p = np.nonzero(propab)
        ent=np.sum([p*np.log2(1.0/p) for p in propab[nonzero_p]])
        return ent
    except Exception as e:
        print(e)
        print(signal)

    

def conv_entropy(signal):
    
    try:
        hist_prob = np.histogram(signal, bins=np.arange(0,1.001,0.001,dtype=float), density=True)[0]
        nonzero_p = np.nonzero(hist_prob)
        ent=np.sum([p*np.log2(1.0/p) for p in hist_prob[nonzero_p]])
        print(ent)
        return ent

    except Exception as e:
        print(e)
