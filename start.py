if __name__ == '__main__':

    RP = ResourcePredictor()
    IAE =  InfoAmountEstimator()
    AP = Analytic_Platform()
    
    ##open every component listening port
    RP.open_listening_port()
    IAE.open_listening_port()
    #AP.open_listening_port()

    ##open every component sending port
    RP.open_sending_port()
    IAE.open_sending_port()
    #AP.open_sending_port()

        