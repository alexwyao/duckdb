
class Op:

    _id = 0

    def __init__(self):
        self.id = Op._id
        Op._id += 1

    def get_name(self):
        return "var_{}".format(self.id)

