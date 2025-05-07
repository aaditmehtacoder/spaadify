import { Server } from "socket.io";
import { Message } from "../models/message.model.js";

export const initalizeSocket = (server) => {
    const io = new Server(server, {
        cors: {
            origin: "http://localhost:3000",
            credentials: true
        }
    });

    const userSockets = new Map(); // { userId: socketId }
    const userActivities = new Map(); // { userId: activity } 

    io.on("connection", (socket) => {
        socket.on("user_connected", (userId) => {
                userSockets.set(userId, socket.id);
                userActivities.set(userId, "Idle");

                // broadcast to all connected sockets that this user just logged in
                io.emit("user_connected", userId);

                socket.emit("users_online", Array.from(userSockets.keys()));

                io.emit("activities", Array.from(userActivities.entries()));
        })

        socket.on("update_activity", ({userId, activity}) => {
            console.log("activity updated", userId, activity);
            userActivities.set(userId, activity);
            io.emit("activity_updated", {userId, activity});
        })

        socket.on("send_message", async (data) => {
            try {
                const { senderId, recieverId, content } = data

                const message = await Message.create({
                    senderId,
                    recieverId,
                    content
                })

                // send to reciever in realtime, if they're online
                const recieverSocketId = userSockets.get(recieverId);
                if (recieverSocketId) {
                    io.to(recieverSocketId).emit("recieve_message", message)
                }

                socket.emit("message_sent", message)
            } catch (error) {
                console.error("Message error:", error);
                socket.emit("message_error", error.message);
            }
        });

        socket.on("disconnect", () => {
            let disconnectedUserId;
            for (const [userId, socketId] of userSockets.entries()) {
                // find disconnected user
                if (socketId === socket.id) {
                    disconnectedUserId = userId;
                    userSockets.delete(userId);
                    userActivities.delete(userId);
                    break;
                }
            }
            if (disconnectedUserId) {
                io.emit("user_disconnected", disconnectedUserId);
            }
        });
    });
};
