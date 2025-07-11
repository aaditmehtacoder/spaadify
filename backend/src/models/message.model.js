import mongoose, { mongo } from "mongoose";

const messageSchema = new mongoose.Schema(
    {
        senderId: { type: String, required: true }, // Clerk user ID
        recieverId: { type: String, required: true }, // Clerk user ID
        content: { type: String, required: true },
    },
    { timestamps: true }
);

export const Message = mongoose.model("Message", messageSchema);
