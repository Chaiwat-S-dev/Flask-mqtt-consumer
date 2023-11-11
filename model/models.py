from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

db = SQLAlchemy()
Base = declarative_base()

class AbstractBaseModel(Base):
    __abstract__ = True

    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime)
    updated_at = db.Column(db.DateTime)
    deleted = db.Column(db.DateTime)
    deleted_by_cascade = db.Column(db.Boolean, default=False, nullable=False)


class Organization(AbstractBaseModel):
    __tablename__ = 'apis_organization'

    code = db.Column(db.String(10), index=True, unique=True)
    title = db.Column(db.String(150), index=True)
    logo = db.Column(db.String(), default=None, nullable=True)
    devices = db.relationship("DeviceModel", back_populates="organization")

    def __repr__(self):
        return f"{self.code} - {self.title}"

    @property
    def to_dict(self):
        return {
            "id": self.id,
            "code": self.code,
            "logo": self.logo,
            "devices": self.devices
        }

    @classmethod
    def find_all(cls):
        organizations = db.session.query(cls).all().order_by()
        return [org.to_dict for org in organizations]

    def save(self) -> None:
        db.session.add(self)
        db.session.commit()

    def delete(self) -> None:
        db.session.delete(self)
        db.session.commit()

class DeviceModel(AbstractBaseModel):
    __tablename__ = 'apis_device'

    title = db.Column(db.String(255), nullable=False)
    mac_address = db.Column(db.String(20), nullable=False, index=True)
    mac_address_parent = db.Column(db.String(20), nullable=False, index=True)
    is_gateway = db.Column(db.Boolean, default=False, nullable=False)
    measurement = db.Column(db.String(255), nullable=True)
    organization_id = db.mapped_column(db.ForeignKey("apis_organization.id"))
    organization = db.relationship("Organization", back_populates="devices")
    bucket = db.Column(db.String(255), nullable=True)
    is_activated = db.Column(db.Boolean, default=False, nullable=False)
    parameters = db.Column(JSONB, default=dict, nullable=False, server_default='{}')
    deadman = db.Column(db.Integer, default=3600, nullable=False)
    period = db.Column(db.Integer, default=60, nullable=False)
    brand = db.Column(db.String(64), nullable=True)
    output_parameters = db.Column(JSONB, default=dict, nullable=False, server_default='{}')
    control_parameters = db.Column(JSONB, default=dict, nullable=False, server_default='{}')
    equipment_id = db.Column(db.String(255), nullable=True)
    app_id = db.Column(db.Integer, default=None, nullable=True)
    
    @property
    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "mac_address": self.mac_address,
            "bucket": self.bucket,
            "measurement": self.measurement,
            "organization": self.organization.code,
            "is_deleted": self.deleted,
            "is_activated": self.is_activated,
            "brand": self.brand
        }
    
    @classmethod
    def find_all(cls):
        devices = db.session.query(cls).all()
        return [org.to_dict for org in devices]
 
    def __repr__(self):
        return f"{self.title}:{self.brand}"
    
    def save(self) -> None:
        db.session.add(self)
        db.session.commit()

    def delete(self) -> None:
        db.session.delete(self)
        db.session.commit()