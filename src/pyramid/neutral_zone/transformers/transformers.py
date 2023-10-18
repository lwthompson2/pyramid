from pyramid.model.model import DynamicImport, BufferData


class Transformer(DynamicImport):
    """Transform values and/or type of Pyramid BufferData, en route from Reader to Trial."""

    def transform(self, data: BufferData) -> BufferData:
        raise NotImplementedError  # pragma: no cover
