if __name__ == '__main__':

    RP = ResourcePredictor()
    SLE =  InfoAmountEstimator()
    AP = Analytic_Platform()
    
    ##open every component listening port
    RP.open_listening_port()
    SLE.open_listening_port()
    #AP.open_listening_port()

    ##open every component sending port
    RP.open_sending_port()
    SLE.open_sending_port()
    #AP.open_sending_port()

        