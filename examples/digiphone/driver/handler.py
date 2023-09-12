import digi

@digi.on.model
def h(model):
    ...

if __name__ == '__main__':
    digi.run()
    digi.message.start_listening()
