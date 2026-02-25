import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.KryoSerializable;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;

import java.nio.ByteBuffer;

class Attribute implements KryoSerializable, Comparable<Attribute> {
    byte type;
    byte[] value;
    transient DataTypeEnum dataType;

    @SuppressWarnings("unused")
    private Attribute() {
    }

    Attribute(byte type) {
        this.type = type;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeByte(type);
        output.writeInt(value.length);
        output.writeBytes(value);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.type = input.readByte();
        this.value = new byte[input.readInt()];
        input.readBytes(value);
        this.dataType = DataTypeEnum.values()[this.type];
    }

    public int getInt() {
        if (dataType == DataTypeEnum.INTEGER) {
            return ByteBuffer.wrap(value).getInt();
        }
        throw new SQLException("Invalid Type " + dataType);
    }

    public float getFloat() {
        if (dataType == DataTypeEnum.FLOAT) {
            return ByteBuffer.wrap(value).getFloat();
        }
        throw new SQLException("Invalid Type " + dataType);
    }

    public String getString() {
        if (dataType == DataTypeEnum.STRING) {
            return new String(value);
        }
        throw new SQLException("Invalid Type " + dataType);
    }

    @Override
    public int compareTo(Attribute o) {
        // If types are different, try numeric comparison if both are INTEGER or FLOAT
        if (this.type != o.type) {
            // Allow comparison between INTEGER and FLOAT
            if ((this.dataType == DataTypeEnum.INTEGER || this.dataType == DataTypeEnum.FLOAT) &&
                (o.dataType == DataTypeEnum.INTEGER || o.dataType == DataTypeEnum.FLOAT)) {
                
                // Convert both to float for comparison
                float thisVal = (this.dataType == DataTypeEnum.INTEGER) ? 
                    (float) this.getInt() : this.getFloat();
                float otherVal = (o.dataType == DataTypeEnum.INTEGER) ? 
                    (float) o.getInt() : o.getFloat();
                
                return Float.compare(thisVal, otherVal);
            }
            
            // For different non-numeric types, throw exception
            throw new SQLException("Cannot compare types: " + this.dataType + " and " + o.dataType);
        }

        // Same type comparison
        try {
            return switch (this.dataType) {
                case INTEGER -> Integer.compare(this.getInt(), o.getInt());
                case FLOAT -> Float.compare(this.getFloat(), o.getFloat());
                case STRING -> this.getString().compareTo(o.getString());
            };
        } catch (Exception e) {
            throw new SQLException("Error comparing attributes: " + e.getMessage());
        }
    }
}
